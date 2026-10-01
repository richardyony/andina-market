"""Gold · modelo estrella para analítica.

Dimensiones: fecha, cliente (SCD2 por segmento) y producto (con miembro "desconocido").
Hechos: líneas de pedido, pedidos y pagos. Todas son vistas materializadas: se recalculan
desde silver en cada corrida (de forma incremental cuando el motor puede), así que son
idempotentes por construcción. Las claves sustitutas son hashes deterministas: la misma
versión de un cliente tiene siempre la misma clave, aunque la tabla se recalcule.
"""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

UNKNOWN_PRODUCT_KEY = -1
MONTHS = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto",
          "septiembre", "octubre", "noviembre", "diciembre"]
DAYS = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]


def read(name: str):
    return spark.read.table(name)  # noqa: F821


def date_key(col):
    return F.date_format(col, "yyyyMMdd").cast("int")


# ---------------------------------------------------------------------------
# Dimensiones
# ---------------------------------------------------------------------------
@dp.materialized_view(name="gold.dim_date", comment="Calendario 2024-2027 (el histórico empieza en oct-2024).")
def dim_date():
    d = spark.sql(  # noqa: F821
        "SELECT explode(sequence(DATE'2024-01-01', DATE'2027-12-31')) AS date"
    )
    dow = F.dayofweek("date")  # 1 = domingo … 7 = sábado
    iso_dow = F.when(dow == 1, 7).otherwise(dow - 1)  # 1 = lunes … 7 = domingo
    return d.select(
        date_key("date").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.element_at(F.array(*[F.lit(m) for m in MONTHS]), F.month("date")).alias("month_name"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.dayofmonth("date").alias("day"),
        iso_dow.alias("day_of_week"),
        F.element_at(F.array(*[F.lit(x) for x in DAYS]), iso_dow).alias("day_name"),
        (iso_dow >= 6).alias("is_weekend"),
    )


@dp.materialized_view(
    name="gold.dim_customer",
    comment="Cliente con historial SCD2 del segmento: una fila por versión. Los demás atributos "
            "son los vigentes (SCD1). La primera versión se abre en 1900-01-01 para cubrir el "
            "histórico anterior a la primera ingesta.",
    cluster_by=["customer_id"],
)
def dim_customer():
    hist = read("silver.customer_segment_history")
    cur = read("silver.customers")
    dups = read("silver.customer_duplicates").select("customer_id", "principal_customer_id")
    first_version = F.col("__START_AT._ct_version") == F.min("__START_AT._ct_version").over(
        Window.partitionBy("customer_id")
    )
    return (
        hist.select(
            F.xxhash64("customer_id", F.col("__START_AT._ct_version")).alias("customer_key"),
            "customer_id",
            "segment",
            F.when(first_version, F.lit("1900-01-01").cast("timestamp"))
             .otherwise(F.col("__START_AT.updated_at")).alias("valid_from"),
            F.coalesce(F.col("__END_AT.updated_at"), F.lit("9999-12-31").cast("timestamp")).alias("valid_to"),
            F.col("__END_AT").isNull().alias("is_current"),
        )
        .join(
            cur.select(
                "customer_id", "first_name", "last_name",
                F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
                "email_norm", "email_valido", "city",
                F.coalesce("country_iso", F.lit("ND")).alias("country"),
                "signup_date",
            ),
            "customer_id",
        )
        .join(dups, "customer_id", "left")
        # Solo las cuentas secundarias se marcan; principal_customer_id permite agrupar a la persona.
        .withColumn("is_possible_duplicate", F.coalesce(F.col("customer_id") != F.col("principal_customer_id"), F.lit(False)))
        .withColumn("principal_customer_id", F.coalesce("principal_customer_id", "customer_id"))
    )


@dp.materialized_view(
    name="gold.dim_product",
    comment="Producto, estado actual. Incluye el miembro -1 'Producto desconocido' para las líneas "
            "en cuarentena, así los totales de venta cuadran.",
)
def dim_product():
    p = read("silver.products").select(
        F.col("product_id").alias("product_key"), "product_id", "sku", "product_name",
        "category", "subcategory", "brand", "price", "status", "is_active",
    )
    unknown = spark.range(1).select(  # noqa: F821
        F.lit(UNKNOWN_PRODUCT_KEY).cast("int").alias("product_key"),
        F.lit(None).cast("int").alias("product_id"),
        F.lit("ND").alias("sku"),
        F.lit("Producto desconocido").alias("product_name"),
        F.lit("Desconocida").alias("category"),
        F.lit(None).cast("string").alias("subcategory"),
        F.lit(None).cast("string").alias("brand"),
        F.lit(None).cast("decimal(12,2)").alias("price"),
        F.lit("Unknown").alias("status"),
        F.lit(False).alias("is_active"),
    )
    return p.unionByName(unknown)


# ---------------------------------------------------------------------------
# Hechos
# ---------------------------------------------------------------------------
def orders_with_customer_key():
    """Pedidos con la versión del cliente vigente en la fecha del pedido (point-in-time)."""
    o = read("silver.orders")
    c = read("gold.dim_customer").select("customer_key", "customer_id", "valid_from", "valid_to")
    return o.join(
        c,
        (o.customer_id == c.customer_id) & (o.order_date >= c.valid_from) & (o.order_date < c.valid_to),
        "left",
    ).drop(c.customer_id)


@dp.materialized_view(
    name="gold.fact_order_lines",
    comment="Grano: una línea de pedido. Medidas de venta por producto.",
    cluster_by=["date_key"],
)
def fact_order_lines():
    items = read("silver.order_items")
    orders = orders_with_customer_key().select(
        "order_id", "order_date", "customer_key", "channel", F.col("status").alias("order_status")
    )
    products = read("silver.products").select("product_id", F.lit(True).alias("_known"))
    return (
        items.join(orders, "order_id")
        .join(products, "product_id", "left")
        .select(
            "order_item_id", "order_id",
            date_key("order_date").alias("date_key"),
            "customer_key",
            F.when(F.col("_known"), F.col("product_id")).otherwise(F.lit(UNKNOWN_PRODUCT_KEY)).alias("product_key"),
            "channel", "order_status",
            "quantity", "unit_price", "line_amount",
        )
    )


@dp.materialized_view(
    name="gold.fact_orders",
    comment="Grano: un pedido. Incluye el total de sus líneas y las banderas de calidad.",
    cluster_by=["date_key"],
)
def fact_orders():
    items = read("silver.order_items").groupBy("order_id").agg(
        F.count("*").alias("items_count"),
        F.sum("quantity").alias("units"),
        F.sum("line_amount").alias("items_total"),
    )
    double = read("silver.payment_double_charges").select("order_id").distinct().withColumn("_dc", F.lit(True))
    return (
        orders_with_customer_key()
        .join(items, "order_id", "left")
        .join(double, "order_id", "left")
        .select(
            "order_id",
            date_key("order_date").alias("date_key"),
            "customer_key", "channel", F.col("status").alias("order_status"), "coupon_code",
            "total_amount",
            F.coalesce("items_count", F.lit(0)).alias("items_count"),
            F.coalesce("units", F.lit(0)).alias("units"),
            "items_total",
            (F.col("total_amount") - F.col("items_total")).alias("total_difference"),
            (F.coalesce("items_count", F.lit(0)) == 0).alias("has_no_lines"),
            "order_date_corrected",
            F.coalesce("_dc", F.lit(False)).alias("has_double_charge"),
        )
    )


@dp.materialized_view(
    name="gold.fact_payments",
    comment="Grano: un pago (estado actual). El recorrido de estados está en silver.payment_status_history.",
    cluster_by=["date_key"],
)
def fact_payments():
    p = read("silver.payments")
    orders = orders_with_customer_key().select("order_id", "customer_key", "channel")
    double = read("silver.payment_double_charges").select(
        F.col("duplicate_payment_id").alias("payment_id"), F.lit(True).alias("_dc")
    )
    return (
        p.join(orders, "order_id", "left")
        .join(double, "payment_id", "left")
        .select(
            "payment_id", "order_id",
            date_key("payment_date").alias("date_key"),
            "customer_key", "channel", "method",
            F.col("status").alias("payment_status"),
            "amount",
            F.coalesce("_dc", F.lit(False)).alias("is_double_charge"),
        )
    )
