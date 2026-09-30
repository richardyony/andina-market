# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · landing → bronze con Auto Loader
# MAGIC
# MAGIC Una tabla Delta por tabla de origen en `<catalog>.bronze`, **append-only**:
# MAGIC cada lote se agrega tal cual llegó, así bronze guarda el historial completo de
# MAGIC cambios (incluidos los DELETE y cada estado de `Payments`).
# MAGIC
# MAGIC - **Auto Loader** (`cloudFiles`) detecta solo los archivos nuevos en landing.
# MAGIC - **Checkpoint** por tabla en el Volume `ops.checkpoints`: registra qué archivos
# MAGIC   ya se procesaron. Si la tarea falla, se reanuda sin duplicar ni perder archivos
# MAGIC   (exactly-once entre landing y bronze).
# MAGIC - **Trigger `availableNow`**: procesa lo pendiente y termina. Es streaming con
# MAGIC   costo de batch; para tiempo real basta cambiar el trigger (ver docs).
# MAGIC - **Evolución de esquema**: `addNewColumns`. Ante una columna nueva el stream se
# MAGIC   detiene, guarda el esquema ampliado y se reinicia (lo hace este notebook).
# MAGIC   Las filas anteriores quedan con la columna en NULL.
# MAGIC - Metadatos de trazabilidad: `_ingested_at`, `_source_file`, más los que trae el
# MAGIC   lote desde la extracción (`_batch_id`, `_ct_version`, `_ct_operation`,
# MAGIC   `_extracted_at`, `_source`).

# COMMAND ----------

dbutils.widgets.text("catalog", "andina_dev")
dbutils.widgets.text("tables", "")  # vacío = todas

# COMMAND ----------

from pyspark.sql import functions as F

from fuentes import CHECKPOINT_VOLUME, LANDING_VOLUME, select_tables

catalog = dbutils.widgets.get("catalog")
tables = select_tables(dbutils.widgets.get("tables"))

# COMMAND ----------

def has_files(path: str) -> bool:
    try:
        return len(dbutils.fs.ls(path)) > 0
    except Exception:  # noqa: BLE001 - la carpeta aún no existe
        return False


def schema_changed(exc: Exception) -> bool:
    msg = str(exc)
    return "UNKNOWN_FIELD_EXCEPTION" in msg or "UnknownFieldException" in msg


def ingest(target: str, max_restarts: int = 3) -> int:
    source = f"/Volumes/{catalog}/landing/{LANDING_VOLUME}/{target}"
    checkpoint = f"/Volumes/{catalog}/ops/{CHECKPOINT_VOLUME}/bronze/{target}"
    table = f"{catalog}.bronze.{target}"
    if not has_files(source):
        print(f"{target:<16} sin lotes en landing todavía")
        return 0

    for attempt in range(max_restarts + 1):
        stream = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", f"{checkpoint}/_schema")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(source)
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
        )
        query = (
            stream.writeStream
            .option("checkpointLocation", checkpoint)
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .queryName(f"bronze_{target}")
            .toTable(table)
        )
        try:
            query.awaitTermination()
            break
        except Exception as exc:  # noqa: BLE001
            if attempt < max_restarts and schema_changed(exc):
                print(f"{target:<16} columna nueva en origen: se reinicia con el esquema ampliado")
                continue
            raise

    # Bronze es un registro histórico: se prohíben UPDATE y DELETE a nivel de tabla.
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.appendOnly' = 'true')")
    spark.sql(f"COMMENT ON TABLE {table} IS 'Bronze append-only de Azure SQL (dbo) vía Change Tracking y Auto Loader'")

    # recentProgress devuelve dicts (Spark Connect) u objetos según la versión.
    rows = sum((p.get("numInputRows") if isinstance(p, dict) else p.numInputRows) or 0
               for p in query.recentProgress)
    print(f"{target:<16} +{rows:>7,} filas → {table}")
    return rows

# COMMAND ----------

total = sum(ingest(t["target"]) for t in tables)
print(f"Total agregado a bronze en esta corrida: {total:,} filas")
