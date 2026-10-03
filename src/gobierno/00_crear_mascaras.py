# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Funciones de máscara para datos personales
# MAGIC
# MAGIC Crea (o actualiza) `<catalog>.ops.mask_name`, `mask_email` y `mask_phone`, que
# MAGIC `silver.customers` usa con la cláusula `MASK`. Va antes del pipeline: en un entorno nuevo,
# MAGIC sin estas funciones, la tabla no se podría crear. Las definiciones están en `mascaras.py` (D-25).
# MAGIC
# MAGIC Si una función ya existe con otro dueño (las primeras se crearon a mano), el service principal
# MAGIC no puede reemplazarla: se comprueba que su definición sea la del repositorio y, si difiere,
# MAGIC la tarea falla para que nadie crea que la máscara cambió.

# COMMAND ----------

dbutils.widgets.text("catalog", "andina_dev")
catalog = dbutils.widgets.get("catalog")

from mascaras import BODIES, body, create_statements


def normalize(sql: str) -> str:
    return " ".join(sql.split()).lower()


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.ops")
for name, stmt in zip(BODIES, create_statements(catalog)):
    try:
        spark.sql(stmt)
        print(f"{name}: creada o actualizada")
    except Exception as e:  # noqa: BLE001
        if "PERMISSION_DENIED" not in str(e):
            raise
        current = spark.sql(f"""
            SELECT routine_definition, routine_owner FROM {catalog}.information_schema.routines
            WHERE routine_schema = 'ops' AND routine_name = '{name}'""").first()
        if current is None or normalize(current.routine_definition) != normalize(body(name)):
            raise RuntimeError(f"{name} existe con otra definición y no se puede reemplazar") from e
        print(f"{name}: ya existe con la definición del repositorio (dueño: {current.routine_owner})")
