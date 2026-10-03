# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Extracción incremental Azure SQL → landing
# MAGIC
# MAGIC Lee `andina_oltp` por JDBC y publica cada lote como Parquet en
# MAGIC `/Volumes/<catalog>/landing/sqlserver/<tabla>/to_v<versión>__<modo>__run_<batch_id>/`.
# MAGIC
# MAGIC - **Carga completa** la primera vez, si la versión guardada ya no es válida
# MAGIC   (retención de Change Tracking vencida) o con `force_full = true`.
# MAGIC - **Incremental** en las demás corridas: `CHANGETABLE(CHANGES ...)` desde la
# MAGIC   última versión extraída, con INSERT, UPDATE y DELETE (los DELETE llegan solo con la PK).
# MAGIC
# MAGIC **Idempotencia: cada rango de versiones se publica una sola vez (D-09).**
# MAGIC 1. El lote se escribe en `_staging` y se mueve completo a landing: Auto Loader nunca
# MAGIC    ve un lote a medias, y lo que quede en `_staging` de un intento fallido se descarta.
# MAGIC 2. La carpeta se nombra por la versión de corte, no por corrida: un reintento nunca
# MAGIC    sobrescribe un lote anterior.
# MAGIC 3. La marca de agua (`ops.ct_watermarks`) se guarda después de publicar. Si el proceso
# MAGIC    cae entre ambos pasos, la siguiente corrida toma la versión del último lote publicado,
# MAGIC    así que no vuelve a extraer ese rango.
# MAGIC
# MAGIC Cada tabla deja una fila en `ops.ingestion_log`, incluido si cambió el esquema de origen.

# COMMAND ----------

dbutils.widgets.text("catalog", "andina_dev")
dbutils.widgets.text("tables", "")  # vacío = todas; ej. "Orders,Payments"
dbutils.widgets.text("batch_id", "")  # el job pasa {{job.run_id}}
dbutils.widgets.dropdown("force_full", "false", ["false", "true"])
# Solo para pruebas de recuperación: nombre de una tabla de origen (ej. "Orders") que falla
# después de publicar su lote y antes de guardar la marca de agua. El job no lo usa.
dbutils.widgets.text("simulate_failure", "")

# COMMAND ----------

import json
import time
from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F

from fuentes import LANDING_VOLUME, SECRET_SCOPE, lot_name, max_published_version, select_tables

catalog = dbutils.widgets.get("catalog")
tables = select_tables(dbutils.widgets.get("tables"))
force_full = dbutils.widgets.get("force_full") == "true"
simulate_failure = dbutils.widgets.get("simulate_failure").strip()
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

LANDING_ROOT = f"/Volumes/{catalog}/landing/{LANDING_VOLUME}"
STAGING_ROOT = f"{LANDING_ROOT}/_staging"  # fuera de las carpetas que lee Auto Loader


def quote(col: str) -> str:
    return f"[{col}]"


def published_version(target: str):
    """Mayor versión de corte ya publicada en landing para la tabla (None si no hay).

    Un lote solo aparece en landing cuando está completo (se mueve desde _staging),
    así que su nombre es una prueba de que esa versión ya se extrajo. Si el proceso
    cayó antes de guardar la marca de agua, de aquí se recupera.
    """
    try:
        entries = dbutils.fs.ls(f"{LANDING_ROOT}/{target}")
    except Exception:  # noqa: BLE001 - la carpeta aún no existe
        return None
    return max_published_version(e.name for e in entries)


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

    # --- Desde qué versión seguir: la marca de agua o, si es mayor, el último lote
    # publicado en landing (caso: el proceso cayó entre publicar y guardar la marca).
    saved = wm.last_version if wm else None
    published = published_version(target)
    candidates = [v for v in (saved, published) if v is not None]
    last = max(candidates) if candidates else None
    recovered = published is not None and (saved is None or published > saved)

    # --- Modo de extracción
    min_valid = scalar(f"SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('dbo.{src}')) AS v")
    if min_valid is None:
        raise ValueError(f"Change Tracking no está activo en dbo.{src}")
    if force_full:
        mode, reason, from_version = "full", "force_full", None
    elif last is None:
        mode, reason, from_version = "full", "primera extracción", None
    elif last < min_valid:
        mode, reason, from_version = "full", f"versión {last} < mínima válida {min_valid}", None
    else:
        mode, from_version = "incremental", last
        reason = f"change tracking (versión {last} recuperada de landing)" if recovered else "change tracking"

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

    # --- Escritura en dos pasos: _staging y luego publicación en landing.
    # Auto Loader solo lee landing/<tabla>/, así que nunca ve un lote a medio escribir.
    # Lo que haya quedado en _staging de un intento fallido se descarta.
    staging_dir = f"{STAGING_ROOT}/{target}"
    dbutils.fs.rm(staging_dir, True)
    name = lot_name(int(to_version), mode, batch_id)
    staging = f"{staging_dir}/{name}"
    df = (
        read_sql(query)
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_extracted_at", F.current_timestamp())
        .withColumn("_source", F.lit(f"azuresql:{SOURCE_DB}.dbo.{src}"))
    )
    df.write.mode("overwrite").parquet(staging)

    # --- Recarga completa: los borrados ocurridos mientras no se extraía no llegan por Change
    # Tracking. Toda clave vigente en bronze que no está en el snapshot nuevo se borró en la
    # fuente: se agrega al mismo lote como un DELETE sintético con la versión de corte.
    synthetic_deletes = 0
    bronze_table = f"{catalog}.bronze.{target}"
    if mode == "full" and spark.catalog.tableExists(bronze_table):
        latest = Window.partitionBy(pk).orderBy(F.col("_ct_version").desc())
        alive = (
            spark.table(bronze_table)
            .withColumn("_rn", F.row_number().over(latest))
            .where("_rn = 1 AND _ct_operation <> 'D'")
            .select(pk)
        )
        gone = alive.join(spark.read.parquet(staging).select(pk), pk, "left_anti")
        snapshot_schema = spark.read.parquet(staging).schema
        deletes = gone.select(
            *[
                F.col(pk) if f.name == pk
                else F.lit(int(to_version)).cast("bigint").alias(f.name) if f.name == "_ct_version"
                else F.lit("D").alias(f.name) if f.name == "_ct_operation"
                else F.lit(batch_id).alias(f.name) if f.name == "_batch_id"
                else F.current_timestamp().alias(f.name) if f.name == "_extracted_at"
                else F.lit(f"azuresql:{SOURCE_DB}.dbo.{src}").alias(f.name) if f.name == "_source"
                else F.lit(None).cast(f.dataType).alias(f.name)
                for f in snapshot_schema.fields
            ]
        )
        deletes.write.mode("append").parquet(staging)
        synthetic_deletes = spark.read.parquet(staging).where("_ct_operation = 'D'").count()
        if synthetic_deletes:
            reason += f"; {synthetic_deletes} borrados detectados contra bronze"

    rows = spark.read.parquet(staging).count()
    path = None
    if rows > 0:
        path = f"{LANDING_ROOT}/{target}/{name}"
        dbutils.fs.mkdirs(f"{LANDING_ROOT}/{target}")
        dbutils.fs.mv(staging, path, recurse=True)  # publicación del lote completo
    dbutils.fs.rm(staging_dir, True)  # sin cambios: no se publica un lote vacío

    if simulate_failure == src:
        raise RuntimeError(f"Falla simulada en {src} después de publicar el lote y antes de guardar la marca de agua")

    # --- Commit: la versión avanza solo después de publicar el lote
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
failures = []
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
    # Registro por tabla (no al final): si el proceso se interrumpe, lo ya hecho queda anotado.
    spark.createDataFrame([{"batch_id": batch_id, **r}], LOG_SCHEMA).write.mode("append").saveAsTable(
        f"{catalog}.ops.ingestion_log")

if failures:
    # La tarea falla para que el job lo muestre y reintente. Las tablas que sí
    # terminaron ya guardaron su versión y el reintento sigue desde ahí.
    raise RuntimeError(f"Falló la extracción de: {failures}")
