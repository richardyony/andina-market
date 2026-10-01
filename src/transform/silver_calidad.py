"""Silver · calidad de datos: cuarentena, detecciones y registro único de problemas.

Principio del proyecto: nunca se descarta nada en silencio. Las expectations dejan métricas en
el event log del pipeline; además, cada problema concreto queda como una fila consultable en
`silver.data_quality_issues`, con la regla, la entidad y el identificador afectado.
"""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = spark.conf.get("andina.catalog")  # noqa: F821 - `spark` lo inyecta el pipeline
DOUBLE_CHARGE_WINDOW_S = 60  # dos aprobaciones del mismo monto en menos de un minuto = doble clic


def read(name: str):
    return spark.read.table(name)  # noqa: F821


# ---------------------------------------------------------------------------
# Cuarentena: líneas cuyo producto no existe en el catálogo
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="silver.quarantine_order_items",
    comment="Líneas con ProductId inexistente (FK WITH NOCHECK de una migración legacy). "
            "Siguen en silver.order_items; en gold apuntan al producto 'desconocido'.",
)
def quarantine_order_items():
    items = read("silver.order_items")
    products = read("silver.products").select("product_id")
    return (
        items.join(products, "product_id", "left_anti")
        .select(
            "order_item_id", "order_id", "product_id", "quantity", "unit_price", "line_amount",
            F.lit("producto_inexistente").alias("reason"),
            "_batch_id",
        )
    )


# ---------------------------------------------------------------------------
# Dobles cobros: el mismo pago aprobado registrado dos veces
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="silver.payment_double_charges",
    comment=f"Pagos aprobados del mismo pedido y monto con menos de {DOUBLE_CHARGE_WINDOW_S} s de "
            "diferencia. El segundo es el cobro duplicado que hay que devolver.",
)
def payment_double_charges():
    w = Window.partitionBy("order_id", "amount").orderBy("payment_date", "payment_id")
    approved = read("silver.payments").where("status = 'Aprobado'")
    return (
        approved.select(
            "order_id", "amount", "method",
            F.col("payment_id").alias("duplicate_payment_id"),
            F.col("payment_date").alias("duplicate_payment_date"),
            F.lag("payment_id").over(w).alias("original_payment_id"),
            F.lag("payment_date").over(w).alias("original_payment_date"),
        )
        .withColumn(
            "seconds_apart",
            F.col("duplicate_payment_date").cast("long") - F.col("original_payment_date").cast("long"),
        )
        .where(f"original_payment_id IS NOT NULL AND seconds_apart <= {DOUBLE_CHARGE_WINDOW_S}")
    )


# ---------------------------------------------------------------------------
# Registro único de problemas de calidad
# ---------------------------------------------------------------------------
def issue(df, rule: str, action: str, entity: str, id_col: str, detail):
    return df.select(
        F.lit(rule).alias("rule"),
        F.lit(action).alias("action"),
        F.lit(entity).alias("entity"),
        F.col(id_col).cast("string").alias("entity_id"),
        detail.cast("string").alias("detail"),
    )


@dp.materialized_view(
    name="silver.data_quality_issues",
    comment="Una fila por problema de calidad detectado: regla, acción tomada, entidad e id. "
            "action: warn (se conserva y se marca), drop (no entra a silver), quarantine (se aparta), info.",
    cluster_by=["rule"],
)
def data_quality_issues():
    customers = read("silver.customers")
    orders = read("silver.orders")
    items = read("silver.order_items")
    tickets = read("silver.support_tickets")
    duplicates = read("silver.customer_duplicates").where("customer_id <> principal_customer_id")
    quarantine = read("silver.quarantine_order_items")
    double = read("silver.payment_double_charges")

    # Líneas con cantidad 0: no están en silver (drop), se leen de su última versión en bronze.
    last = Window.partitionBy("OrderItemId").orderBy(F.col("_ct_version").desc())
    zero_qty = (
        read(f"{CATALOG}.bronze.order_items")
        .withColumn("rn", F.row_number().over(last))
        .where("rn = 1 AND _ct_operation <> 'D' AND Quantity = 0")
    )
    orders_with_zero_qty = zero_qty.select(F.col("OrderId").alias("order_id")).distinct().withColumn("_q0", F.lit(True))

    items_total = items.groupBy("order_id").agg(F.sum("line_amount").alias("items_total"))
    # La causa importa para actuar: un total menor que las líneas es un descuento aplicado solo a la
    # cabecera; uno mayor, casi siempre una línea con cantidad 0 que silver excluye.
    with_totals = (
        orders.join(items_total, "order_id")
        .join(orders_with_zero_qty, "order_id", "left")
        .withColumn(
            "cause",
            F.when(F.col("_q0"), F.lit("línea con cantidad 0 excluida"))
             .when(F.col("total_amount") < F.col("items_total"), F.lit("descuento en la cabecera"))
             .otherwise(F.lit("sin causa conocida")),
        )
    )
    without_lines = (
        orders.join(items.select("order_id").distinct(), "order_id", "left_anti")
        .join(orders_with_zero_qty, "order_id", "left")
        .withColumn(
            "cause",
            F.when(F.col("_q0"), F.lit("solo tenía líneas con cantidad 0")).otherwise(F.lit("sin líneas en la fuente")),
        )
    )

    parts = [
        issue(customers.where("email IS NOT NULL AND NOT email_valido"),
              "email_formato_invalido", "warn", "customer", "customer_id", F.col("email")),
        issue(customers.where("country_raw IS NOT NULL AND country_iso IS NULL"),
              "pais_no_reconocido", "warn", "customer", "customer_id", F.col("country_raw")),
        issue(customers.where("country_iso IS NOT NULL AND country_raw <> country_iso"),
              "pais_normalizado", "info", "customer", "customer_id",
              F.concat_ws(" → ", F.concat(F.lit("'"), "country_raw", F.lit("'")), "country_iso")),
        issue(duplicates, "cliente_duplicado", "warn", "customer", "customer_id",
              F.concat(F.lit("principal: "), F.col("principal_customer_id"),
                       F.lit(" · regla: "), F.array_join("match_rules", ", "))),
        issue(orders.where("order_date_corrected"),
              "fecha_pedido_futura", "warn", "order", "order_id",
              F.concat(F.lit("original: "), F.col("order_date_raw"), F.lit(" · usada: "), F.col("order_date"))),
        issue(with_totals.where(F.abs(F.col("total_amount") - F.col("items_total")) > 0.01),
              "total_no_cuadra_con_lineas", "warn", "order", "order_id",
              F.concat(F.col("cause"), F.lit(" · total: "), F.col("total_amount"),
                       F.lit(" · líneas: "), F.col("items_total"))),
        issue(without_lines, "pedido_sin_lineas", "warn", "order", "order_id",
              F.concat(F.col("cause"), F.lit(" · estado: "), F.col("status"))),
        issue(zero_qty, "cantidad_cero", "drop", "order_item", "OrderItemId",
              F.concat(F.lit("pedido: "), F.col("OrderId"))),
        issue(quarantine, "producto_inexistente", "quarantine", "order_item", "order_item_id",
              F.concat(F.lit("product_id: "), F.col("product_id"))),
        issue(double, "doble_cobro", "warn", "payment", "duplicate_payment_id",
              F.concat(F.lit("original: "), F.col("original_payment_id"),
                       F.lit(" · segundos: "), F.col("seconds_apart"))),
        issue(tickets.where("body IS NULL"),
              "ticket_sin_cuerpo", "info", "support_ticket", "ticket_id", F.col("subject")),
    ]
    result = parts[0]
    for part in parts[1:]:
        result = result.unionByName(part)
    return result
