"""Gold · capa analítica: agregados con la definición única de cada KPI.

El dashboard solo lee estas tablas. Así la definición de "venta neta" o "recompra" vive en un
solo lugar, versionada con el código, y no en la consulta de cada gráfico (D-18).

Todas las medidas son aditivas (sumas y conteos): los ratios se calculan en el dashboard como
cociente de sumas, para que el total de varios países o meses sea correcto. Nunca se promedian
porcentajes.
"""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

# Venta efectiva: el pedido se pagó y no se canceló ni se devolvió.
EFFECTIVE = ["Pagado", "Enviado", "Entregado"]
# Para la tasa de devolución, el universo es todo lo que llegó a venderse (incluye lo devuelto).
SOLD = EFFECTIVE + ["Devuelto"]
REPURCHASE_DAYS = 90


def read(name: str):
    return spark.read.table(name)  # noqa: F821


def month(date_key_col):
    return F.date_format(F.to_date(date_key_col.cast("string"), "yyyyMMdd"), "yyyy-MM")


# ---------------------------------------------------------------------------
# KPI 1 · Ventas netas y ticket promedio
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="gold.agg_sales_monthly",
    comment="Ventas netas por mes, país, canal y segmento (del cliente en la fecha del pedido). "
            "Venta neta = total cobrado de pedidos Pagados, Enviados o Entregados. "
            "Ticket promedio = net_sales / orders (calcular como cociente de sumas).",
    cluster_by=["year_month"],
)
def agg_sales_monthly():
    o = read("gold.fact_orders").where(F.col("order_status").isin(EFFECTIVE))
    c = read("gold.dim_customer").select("customer_key", "country", "segment")
    return (
        o.join(c, "customer_key")
        .groupBy(month(F.col("date_key")).alias("year_month"), "country", "channel", "segment")
        .agg(
            F.sum("total_amount").alias("net_sales"),
            F.count("*").alias("orders"),
            F.sum("units").alias("units"),
        )
    )


# ---------------------------------------------------------------------------
# KPI 2 · Recompra a 90 días por cohorte
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="gold.agg_repurchase_cohorts",
    comment=f"Cohorte = mes de la primera compra efectiva de la persona. Recompra = otra compra "
            f"efectiva dentro de {REPURCHASE_DAYS} días. Se cuenta por persona (principal_customer_id), "
            "no por cuenta: las cuentas duplicadas no inflan las cohortes. is_complete = false si "
            f"aún no pasaron {REPURCHASE_DAYS} días desde el fin del mes (la tasa todavía puede subir).",
)
def agg_repurchase_cohorts():
    o = read("gold.fact_orders").where(F.col("order_status").isin(EFFECTIVE))
    person = read("gold.dim_customer").select("customer_key", "principal_customer_id", "country")
    orders = (
        o.join(person, "customer_key")
        .select("principal_customer_id", "country",
                F.to_date(F.col("date_key").cast("string"), "yyyyMMdd").alias("order_date"))
    )
    w = Window.partitionBy("principal_customer_id").orderBy("order_date")
    ranked = orders.withColumn("n", F.row_number().over(w))
    first = ranked.where("n = 1").select(
        "principal_customer_id", "country", F.col("order_date").alias("first_date")
    )
    later = ranked.where("n > 1").select("principal_customer_id", F.col("order_date").alias("next_date"))
    repurchased = (
        first.join(later, "principal_customer_id")
        .where(F.col("next_date") <= F.date_add("first_date", REPURCHASE_DAYS))
        .select("principal_customer_id").distinct()
        .withColumn("repurchased", F.lit(1))
    )
    last_date = orders.agg(F.max("order_date").alias("last_date"))
    return (
        first.join(repurchased, "principal_customer_id", "left")
        .crossJoin(last_date)
        .groupBy(F.date_format("first_date", "yyyy-MM").alias("cohort_month"), "country")
        .agg(
            F.count("*").alias("new_customers"),
            F.sum(F.coalesce("repurchased", F.lit(0))).alias("repurchased_90d"),
            (F.date_add(F.last_day(F.max("first_date")), REPURCHASE_DAYS) <= F.max("last_date")).alias("is_complete"),
        )
    )


# ---------------------------------------------------------------------------
# KPI 3 · Salud de cobros
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="gold.agg_payments_monthly",
    comment="Intentos de pago por mes, canal y método. Aprobado = estado actual Aprobado o "
            "Reembolsado (un reembolso implica que se aprobó). Tasa de aprobación = approved / "
            "(approved + rejected). Dobles cobros: total y los que siguen sin devolver.",
    cluster_by=["year_month"],
)
def agg_payments_monthly():
    p = read("gold.fact_payments")
    approved = F.col("payment_status").isin("Aprobado", "Reembolsado")
    return p.groupBy(month(F.col("date_key")).alias("year_month"), "channel", "method").agg(
        F.count("*").alias("attempts"),
        F.sum(approved.cast("int")).alias("approved"),
        F.sum((F.col("payment_status") == "Rechazado").cast("int")).alias("rejected"),
        F.sum(F.col("is_double_charge").cast("int")).alias("double_charges"),
        F.sum(F.when(F.col("is_double_charge") & (F.col("payment_status") == "Aprobado"), 1).otherwise(0))
         .alias("double_charges_pending"),
        F.sum(F.when(F.col("is_double_charge") & (F.col("payment_status") == "Aprobado"), F.col("amount"))
              .otherwise(0)).alias("double_charge_amount_pending"),
    )


# ---------------------------------------------------------------------------
# KPI 4 · Tasa de devolución por categoría
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="gold.agg_returns_monthly",
    comment="Unidades vendidas y devueltas por mes y categoría. Vendidas = pedidos Pagados, Enviados, "
            "Entregados o Devueltos; devueltas = pedidos Devueltos (la devolución es del pedido completo). "
            "Tasa = returned_units / sold_units.",
    cluster_by=["year_month"],
)
def agg_returns_monthly():
    lines = read("gold.fact_order_lines").where(F.col("order_status").isin(SOLD))
    products = read("gold.dim_product").select("product_key", "category")
    returned = F.col("order_status") == "Devuelto"
    return (
        lines.join(products, "product_key")
        .groupBy(month(F.col("date_key")).alias("year_month"), "category")
        .agg(
            F.sum("quantity").alias("sold_units"),
            F.sum(F.when(returned, F.col("quantity")).otherwise(0)).alias("returned_units"),
            F.sum("line_amount").alias("sold_amount"),
            F.sum(F.when(returned, F.col("line_amount")).otherwise(0)).alias("returned_amount"),
        )
    )
