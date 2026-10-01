# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Puntuación batch con los modelos `champion`
# MAGIC
# MAGIC `fe.score_batch` recibe solo las claves (`customer_id` + fecha, o `ticket_id`): el modelo
# MAGIC registrado sabe qué features necesita y de qué tablas, y las busca solo, con la misma lógica
# MAGIC point-in-time que en el entrenamiento. Así no hay diferencias entre entrenar y puntuar.
# MAGIC
# MAGIC | Salida | Contenido |
# MAGIC |---|---|
# MAGIC | `ml.repurchase_scores` | Probabilidad de recompra en 90 días de cada cliente con compras, a hoy |
# MAGIC | `ml.urgent_ticket_scores` | Probabilidad de urgencia de cada ticket abierto o en proceso |

# COMMAND ----------

# MAGIC %pip install -q databricks-feature-engineering scikit-learn mlflow
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "andina_dev")
catalog = dbutils.widgets.get("catalog")

import mlflow
from databricks.feature_engineering import FeatureEngineeringClient
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F

fe = FeatureEngineeringClient()
mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()


def champion(name):
    v = client.get_model_version_by_alias(name, "champion").version
    return f"models:/{name}@champion", v

# COMMAND ----------

# Recompra: clientes con al menos una compra en su última foto, puntuados a la fecha actual.
rep_name = f"{catalog}.ml.repurchase_propensity"
rep_uri, rep_version = champion(rep_name)
latest = spark.table(f"{catalog}.ml.customer_features").agg(F.max("as_of_ts")).first()[0]
customers = (
    spark.table(f"{catalog}.ml.customer_features")
    .where((F.col("as_of_ts") == F.lit(latest)) & (F.col("orders_lifetime") > 0))
    # La columna de fecha debe llamarse como la timestamp_lookup_key del entrenamiento (obs_ts).
    .select("customer_id", F.current_timestamp().alias("obs_ts"))
)
rep_scores = (
    fe.score_batch(model_uri=rep_uri, df=customers)
    .select("customer_id", F.col("obs_ts").alias("scored_at"), F.col("prediction").alias("repurchase_probability"),
            F.lit(int(rep_version)).alias("model_version"))
)
rep_scores.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.ml.repurchase_scores")
print(f"repurchase_scores: {spark.table(f'{catalog}.ml.repurchase_scores').count():,} clientes (modelo v{rep_version})")

# COMMAND ----------

# Tickets urgentes: tickets abiertos o en proceso, con la foto del cliente anterior a su creación.
urg_name = f"{catalog}.ml.urgent_ticket_classifier"
urg_uri, urg_version = champion(urg_name)
open_tickets = (
    spark.table(f"{catalog}.silver.support_tickets")
    .where(F.col("status").isin("Abierto", "EnProceso"))
    .select(F.col("ticket_id").cast("int"), F.col("customer_id").cast("int"), "created_at")
)
urg_scores = (
    fe.score_batch(model_uri=urg_uri, df=open_tickets)
    .select("ticket_id", "customer_id", "created_at",
            F.col("prediction").alias("urgency_probability"),
            F.current_timestamp().alias("scored_at"),
            F.lit(int(urg_version)).alias("model_version"))
)
urg_scores.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.ml.urgent_ticket_scores")
print(f"urgent_ticket_scores: {spark.table(f'{catalog}.ml.urgent_ticket_scores').count():,} tickets (modelo v{urg_version})")
