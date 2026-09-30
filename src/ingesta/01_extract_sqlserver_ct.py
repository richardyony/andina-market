# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Extracción incremental Azure SQL → landing
# MAGIC
# MAGIC Lee `andina_oltp` por JDBC y deja cada lote como Parquet en
# MAGIC `/Volumes/<catalog>/landing/sqlserver/<tabla>/<batch_id>/`.
# MAGIC
# MAGIC - **Carga completa** la primera vez, si la versión guardada ya no es válida
# MAGIC   (retención de Change Tracking vencida) o con `force_full = true`.
# MAGIC - **Incremental** en las demás corridas: `CHANGETABLE(CHANGES ...)` desde la
# MAGIC   última versión guardada, con INSERT, UPDATE y DELETE (los DELETE llegan solo con la PK).
# MAGIC - La versión se guarda en `ops.ct_watermarks` **después** de escribir el lote:
# MAGIC   si algo falla antes, la próxima corrida repite desde la versión anterior.
# MAGIC   La extracción es *at-least-once*; bronze y silver deduplican por PK y `_ct_version`.
# MAGIC - Cada tabla deja una fila en `ops.ingestion_log`, incluido si cambió el esquema de origen.

# COMMAND ----------

dbutils.widgets.text("catalog", "andina_dev")
dbutils.widgets.text("tables", "")  # vacío = todas; ej. "Orders,Payments"
dbutils.widgets.text("batch_id", "")  # el job pasa {{job.run_id}}
dbutils.widgets.dropdown("force_full", "false", ["false", "true"])

# COMMAND ----------

import json
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from fuentes import LANDING_VOLUME, SECRET_SCOPE, select_tables

catalog = dbutils.widgets.get("catalog")
tables = select_tables(dbutils.widgets.get("tables"))
force_full = dbutils.widgets.get("force_full") == "true"
batch_id = dbutils.widgets.get("batch_id") or "manual_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

print(f"catalog={catalog} batch_id={batch_id} force_full={force_full} tablas={[t['source'] for t in tables]}")

# COMMAND ----------

# MAGIC %md ## Conexión a la fuente

# COMMAND ----------

def secret(key: str) -> str:
    return dbutils.secrets.get(SECRET_SCOPE, key)


SOURCE_DB = secret("database")
JDBC_URL = (
    f"jdbc:sqlserver://{secret('server')}:1433;database={SOURCE_DB};"
    "encrypt=true;trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;loginTimeout=60"
)


def read_sql(query: str):
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("query", query)
        .option("user", secret("user"))
        .option("password", secret("password"))
        .load()
    )


# Errores transitorios de Azure SQL serverless: base pausada o reanudándose.
TRANSIENT = ("40613", "40501", "40197", "timed out", "Connection reset")


def scalar(query: str, retries: int = 6, wait_s: int = 20):
    """Primer valor de una consulta. Reintenta mientras la base sale de la auto-pausa."""
    for attempt in range(1, retries + 1):
        try:
            return read_sql(query).collect()[0][0]
        except Exception as exc:  # noqa: BLE001 - se filtra por mensaje
            if attempt == retries or not any(code in str(exc) for code in TRANSIENT):
                raise
            print(f"  Base no disponible (¿auto-pausa?), reintento {attempt}/{retries} en {wait_s}s")
            time.sleep(wait_s)

# COMMAND ----------

# MAGIC %md ## Tablas de control y Volumes (idempotente)

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.landing.{LANDING_VOLUME} "
          "COMMENT 'Lotes crudos extraídos de Azure SQL (Parquet), uno por tabla y batch_id'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.ops.checkpoints "
          "COMMENT 'Checkpoints y esquemas inferidos de Auto Loader'")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.ops.ct_watermarks (
    source_table   STRING    COMMENT 'Tabla de origen (dbo)',
    last_version   BIGINT    COMMENT 'Última versión de Change Tracking extraída con éxito',
    source_columns STRING    COMMENT 'Columnas de origen en la última extracción (JSON)',
    last_batch_id  STRING,
    updated_at     TIMESTAMP
) COMMENT 'Marca de agua de Change Tracking por tabla: desde dónde sigue la próxima extracción'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.ops.ingestion_log (
    batch_id        STRING,
    source_table    STRING,
    mode            STRING    COMMENT 'full | incremental',
    mode_reason     STRING,
    from_version    BIGINT,
    to_version      BIGINT,
    rows_extracted  BIGINT,
    landing_path    STRING,
    schema_changed  BOOLEAN,
    added_columns   STRING,
    removed_columns STRING,
    status          STRING    COMMENT 'OK | FAILED',
    error           STRING,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP
) COMMENT 'Registro de cada extracción por tabla y lote (trazabilidad de la ingesta)'
""")

# COMMAND ----------

# MAGIC %md ## Extracción por tabla

# COMMAND ----------

watermarks = {
    r.source_table: r
    for r in spark.table(f"{catalog}.ops.ct_watermarks").collect()
}

# Una sola versión de corte para todo el lote: todas las tablas quedan alineadas
# al mismo punto de la fuente (se leen los cambios con versión <= to_version).
to_version = scalar("SELECT CHANGE_TRACKING_CURRENT_VERSION() AS v")
print(f"Versión actual de Change Tracking: {to_version}")


def quote(col: str) -> str:
    return f"[{col}]"


def extract(t: dict) -> dict:
    src, pk, target = t["source"], t["pk"], t["target"]
    started = datetime.now(timezone.utc)
    # Spark envuelve la consulta en una subconsulta, donde SQL Server no admite
    # ORDER BY: se trae la posición y se ordena aquí.
    cols = [
        r.COLUMN_NAME
        for r in sorted(
            read_sql(
                "SELECT COLUMN_NAME, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{src}'"
            ).collect(),
            key=lambda r: r.ORDINAL_POSITION,
        )
    ]
    if not cols:
        raise ValueError(f"dbo.{src} no existe en la fuente")

    # --- Evolución de esquema: se compara contra las columnas de la extracción anterior
    wm = watermarks.get(src)
    prev_cols = json.loads(wm.source_columns) if wm and wm.source_columns else cols
    added = [c for c in cols if c not in prev_cols]
    removed = [c for c in prev_cols if c not in cols]

    # --- Modo de extracción
    min_valid = scalar(f"SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('dbo.{src}')) AS v")
    if min_valid is None:
        raise ValueError(f"Change Tracking no está activo en dbo.{src}")
    if force_full:
        mode, reason, from_version = "full", "force_full", None
    elif wm is None:
        mode, reason, from_version = "full", "primera extracción", None
    elif wm.last_version < min_valid:
        mode, reason, from_version = "full", f"versión {wm.last_version} < mínima válida {min_valid}", None
    else:
        mode, reason, from_version = "incremental", "change tracking", wm.last_version

    select_cols = ", ".join(f"t.{quote(c)}" for c in cols if c != pk)
    if mode == "full":
        # Snapshot: todas las filas con la versión de corte. Si una fila cambia durante
        # la lectura, vuelve a llegar en el próximo incremental con una versión mayor.
        query = (
            f"SELECT t.{quote(pk)}, {select_cols}, "
            f"CAST({to_version} AS BIGINT) AS _ct_version, 'S' AS _ct_operation "
            f"FROM dbo.{quote(src)} AS t"
        )
    else:
        # LEFT JOIN: en un DELETE la fila ya no existe y solo queda la PK de CHANGETABLE.
        query = (
            f"SELECT ct.{quote(pk)}, {select_cols}, "
            "ct.SYS_CHANGE_VERSION AS _ct_version, ct.SYS_CHANGE_OPERATION AS _ct_operation "
            f"FROM CHANGETABLE(CHANGES dbo.{quote(src)}, {from_version}) AS ct "
            f"LEFT JOIN dbo.{quote(src)} AS t ON t.{quote(pk)} = ct.{quote(pk)} "
            f"WHERE ct.SYS_CHANGE_VERSION <= {to_version}"
        )

    path = f"/Volumes/{catalog}/landing/{LANDING_VOLUME}/{target}/{batch_id}"
    df = (
        read_sql(query)
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_extracted_at", F.current_timestamp())
        .withColumn("_source", F.lit(f"azuresql:{SOURCE_DB}.dbo.{src}"))
    )
    # overwrite: si el job reintenta la tarea con el mismo batch_id, el lote se reemplaza.
    df.write.mode("overwrite").parquet(path)
    rows = spark.read.parquet(path).count()
    if rows == 0:
        dbutils.fs.rm(path, True)  # sin cambios: no se deja un lote vacío en landing
        path = None

    # --- Commit: la versión avanza solo después de escribir el lote
    spark.sql(
        f"""
        MERGE INTO {catalog}.ops.ct_watermarks AS w
        USING (SELECT :src AS source_table, :v AS last_version, :cols AS source_columns,
                      :batch AS last_batch_id, current_timestamp() AS updated_at) AS s
        ON w.source_table = s.source_table
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """,
        args={"src": src, "v": int(to_version), "cols": json.dumps(cols), "batch": batch_id},
    )
    return {
        "source_table": src, "mode": mode, "mode_reason": reason,
        "from_version": from_version, "to_version": int(to_version), "rows_extracted": rows,
        "landing_path": path, "schema_changed": bool(added or removed),
        "added_columns": ",".join(added) or None, "removed_columns": ",".join(removed) or None,
        "status": "OK", "error": None, "started_at": started,
        "finished_at": datetime.now(timezone.utc),
    }

# COMMAND ----------

LOG_SCHEMA = spark.table(f"{catalog}.ops.ingestion_log").schema
results, failures = [], []
for t in tables:
    try:
        r = extract(t)
        flag = f"  ¡esquema! +{r['added_columns']} -{r['removed_columns']}" if r["schema_changed"] else ""
        print(f"{t['source']:<15} {r['mode']:<12} v{r['from_version']}→v{r['to_version']}  {r['rows_extracted']:>7,} filas{flag}")
    except Exception as exc:  # noqa: BLE001 - se registra y se sigue con las demás tablas
        r = {
            "source_table": t["source"], "mode": None, "mode_reason": None, "from_version": None,
            "to_version": int(to_version), "rows_extracted": None, "landing_path": None,
            "schema_changed": None, "added_columns": None, "removed_columns": None,
            "status": "FAILED", "error": str(exc)[:4000],
            "started_at": datetime.now(timezone.utc), "finished_at": datetime.now(timezone.utc),
        }
        failures.append(t["source"])
        print(f"{t['source']:<15} FALLÓ: {str(exc)[:300]}")
    results.append({"batch_id": batch_id, **r})

spark.createDataFrame(results, LOG_SCHEMA).write.mode("append").saveAsTable(f"{catalog}.ops.ingestion_log")

if failures:
    # La tarea falla para que el job lo muestre y reintente; las tablas que sí
    # terminaron ya guardaron su versión y no se vuelven a extraer.
    raise RuntimeError(f"Falló la extracción de: {failures}")
