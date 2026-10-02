# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Tablas de features (Feature Engineering en Unity Catalog)
# MAGIC
# MAGIC Publica dos tablas de features reutilizables por cualquier modelo:
# MAGIC
# MAGIC | Tabla | Clave | Contenido |
# MAGIC |---|---|---|
# MAGIC | `ml.customer_features` | `customer_id` + `as_of_ts` (**clave de tiempo**) | Foto semanal del comportamiento de cada cliente, sumando todas las cuentas de la misma persona |
# MAGIC | `ml.ticket_features` | `ticket_id` | Texto y atributos de cada ticket al momento de crearse |
# MAGIC
# MAGIC **Point-in-time:** la foto de un cliente en `as_of_ts` se calcula solo con hechos **anteriores**
# MAGIC a esa fecha (pedidos, tickets, pagos rechazados). Al entrenar, `timestamp_lookup_key` trae la
# MAGIC foto más reciente que no supera la fecha de cada observación, así un modelo nunca ve el futuro.
# MAGIC
# MAGIC Idempotente: se recalcula todo y se escribe con `merge` por clave primaria.

# COMMAND ----------

# MAGIC %pip install -q databricks-feature-engineering
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "andina_dev")
catalog = dbutils.widgets.get("catalog")

from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import Window
from pyspark.sql import functions as F

fe = FeatureEngineeringClient()
s = f"{catalog}.silver"
EFFECTIVE = ["Pagado", "Enviado", "Entregado"]

# COMMAND ----------

# MAGIC %md ## Fotos semanales: un `as_of_ts` por lunes, desde el primer pedido

# COMMAND ----------

orders = (
    spark.table(f"{s}.orders")
    .where(F.col("status").isin(EFFECTIVE))
    .select("order_id", "customer_id", F.col("order_date").alias("ts"), "total_amount", "channel")
)
bounds = orders.agg(F.min("ts").alias("lo"), F.max("ts").alias("hi")).first()
snapshots = spark.sql(f"""
    SELECT explode(sequence(
        date_trunc('week', TIMESTAMP'{bounds.lo}') + INTERVAL 7 DAYS,
        date_trunc('week', TIMESTAMP'{bounds.hi}') + INTERVAL 7 DAYS,
        INTERVAL 7 DAYS)) AS as_of_ts""")

# Las features son de la PERSONA, no de la cuenta: si alguien tiene dos cuentas (por ejemplo, una
# creada en tienda), sus pedidos, tickets y pagos se suman como un solo cliente. La clave de la
# tabla sigue siendo customer_id: cada cuenta recibe las features de su persona.
customers = spark.table(f"{s}.customers").select("customer_id", "signup_date")
duplicates = spark.table(f"{s}.customer_duplicates").select("customer_id", "principal_customer_id")
account_person = (
    customers.join(duplicates, "customer_id", "left")
    .select("customer_id", "signup_date",
            F.coalesce("principal_customer_id", "customer_id").alias("person_id"))
)
person_signup = account_person.groupBy("person_id").agg(F.min("signup_date").alias("signup_date"))
# Una persona aparece desde la primera foto posterior a su primer registro.
grid = person_signup.crossJoin(snapshots).where(F.col("signup_date") < F.to_date("as_of_ts"))
to_person = account_person.select("customer_id", "person_id")
orders = orders.join(to_person, "customer_id").drop("customer_id")


def window_count(cond, days):
    return F.sum(F.when(cond & (F.col("ts") >= F.col("as_of_ts") - F.expr(f"INTERVAL {days} DAYS")), 1).otherwise(0))

# COMMAND ----------

# MAGIC %md ## Features de pedidos (solo pedidos efectivos anteriores a la foto)

# COMMAND ----------

past_orders = grid.join(orders, "person_id").where(F.col("ts") < F.col("as_of_ts"))
order_feats = past_orders.groupBy("person_id", "as_of_ts").agg(
    window_count(F.lit(True), 90).alias("orders_90d"),
    window_count(F.lit(True), 365).alias("orders_365d"),
    F.sum(F.when(F.col("ts") >= F.col("as_of_ts") - F.expr("INTERVAL 365 DAYS"), F.col("total_amount"))).alias("net_sales_365d"),
    window_count(F.col("channel") == "app", 365).alias("_app_orders_365d"),
    F.max("ts").alias("_last_order_ts"),
    F.count("*").alias("orders_lifetime"),
)

# Categoría favorita del último año (por unidades).
items = spark.table(f"{s}.order_items").select("order_id", "product_id", "quantity")
products = spark.table(f"{s}.products").select("product_id", "category")
cat_units = (
    past_orders.where(F.col("ts") >= F.col("as_of_ts") - F.expr("INTERVAL 365 DAYS"))
    .join(items, "order_id").join(products, "product_id")
    .groupBy("person_id", "as_of_ts", "category").agg(F.sum("quantity").alias("u"))
)
w_cat = Window.partitionBy("person_id", "as_of_ts").orderBy(F.col("u").desc(), F.col("category"))
fav_cat = (
    cat_units.withColumn("rn", F.row_number().over(w_cat)).where("rn = 1")
    .select("person_id", "as_of_ts", F.col("category").alias("favorite_category_365d"))
)

# COMMAND ----------

# MAGIC %md ## Features de soporte y de cobros

# COMMAND ----------

tickets = (
    spark.table(f"{s}.support_tickets").select("customer_id", F.col("created_at").alias("ts"), "priority")
    .join(to_person, "customer_id").drop("customer_id")
)
ticket_feats = (
    grid.join(tickets, "person_id").where(F.col("ts") < F.col("as_of_ts"))
    .groupBy("person_id", "as_of_ts").agg(
        window_count(F.lit(True), 90).alias("tickets_90d"),
        window_count(F.col("priority") == "Urgente", 90).alias("urgent_tickets_90d"),
    )
)

# Pagos rechazados: desde el historial de estados, con la fecha en que el pago pasó a Rechazado.
# Usar el estado actual filtraría el futuro (un pago Pendiente a la fecha de la foto pudo
# rechazarse días después).
rejections = (
    spark.table(f"{s}.payment_status_history").where("status = 'Rechazado'")
    .select("order_id", F.col("__START_AT.updated_at").alias("ts"))
    .join(spark.table(f"{s}.orders").select("order_id", "customer_id"), "order_id")
    .join(to_person, "customer_id").drop("customer_id")
)
payment_feats = (
    grid.join(rejections, "person_id").where(F.col("ts") < F.col("as_of_ts"))
    .groupBy("person_id", "as_of_ts").agg(window_count(F.lit(True), 90).alias("rejected_payments_90d"))
)

# COMMAND ----------

# MAGIC %md ## Ensamblado y publicación

# COMMAND ----------

customer_features = (
    grid.join(order_feats, ["person_id", "as_of_ts"], "left")
    .join(fav_cat, ["person_id", "as_of_ts"], "left")
    .join(ticket_feats, ["person_id", "as_of_ts"], "left")
    .join(payment_feats, ["person_id", "as_of_ts"], "left")
    # Cada cuenta de la persona recibe la misma fila de features.
    .join(to_person, "person_id")
    .select(
        F.col("customer_id").cast("int").alias("customer_id"),
        "as_of_ts",
        F.datediff(F.to_date("as_of_ts"), "signup_date").alias("tenure_days"),
        F.datediff(F.to_date("as_of_ts"), F.to_date("_last_order_ts")).alias("days_since_last_order"),
        *[F.coalesce(c, F.lit(0)).cast("int").alias(c) for c in
          ["orders_90d", "orders_365d", "orders_lifetime", "tickets_90d", "urgent_tickets_90d", "rejected_payments_90d"]],
        F.coalesce("net_sales_365d", F.lit(0)).cast("double").alias("net_sales_365d"),
        # Sin compras en el último año el ticket y la proporción no existen: quedan nulos, no 0.
        F.try_divide("net_sales_365d", "orders_365d").cast("double").alias("avg_order_value_365d"),
        F.try_divide("_app_orders_365d", "orders_365d").cast("double").alias("app_share_365d"),
        "favorite_category_365d",
    )
)

ticket_features = spark.table(f"{s}.support_tickets").select(
    F.col("ticket_id").cast("int").alias("ticket_id"),
    "customer_id",
    "created_at",
    "channel",
    "subject",
    F.coalesce("body", F.lit("")).alias("body"),
    F.length(F.coalesce("body", F.lit(""))).alias("body_length"),
    F.col("order_id").isNotNull().cast("int").alias("has_order"),
    F.hour("created_at").alias("created_hour"),
)


def publish(name, df, primary_keys, description, timeseries=None):
    full = f"{catalog}.ml.{name}"
    if spark.catalog.tableExists(full):
        fe.write_table(name=full, df=df, mode="merge")
    else:
        fe.create_table(name=full, primary_keys=primary_keys, timeseries_columns=timeseries,
                        df=df, description=description)
    print(f"{full}: {spark.table(full).count():,} filas")


publish(
    "customer_features", customer_features, ["customer_id", "as_of_ts"],
    "Foto semanal por cliente. Cada fila usa solo hechos anteriores a as_of_ts (point-in-time). "
    "Pedir con FeatureLookup(lookup_key='customer_id', timestamp_lookup_key=<fecha de la observación>).",
    timeseries=["as_of_ts"],
)
publish(
    "ticket_features", ticket_features, ["ticket_id"],
    "Texto y atributos de cada ticket al crearse. Para el modelo de tickets urgentes.",
)
