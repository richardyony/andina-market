"""Silver · calidad de datos: cuarentena, detecciones y registro único de problemas.

Principio del proyecto: nunca se descarta nada en silencio. Las expectations dejan métricas en
el event log del pipeline; además, cada problema concreto queda como una fila consultable en
`silver.data_quality_issues`, con la regla, la acción, la entidad y el identificador afectado.
La tabla de problemas no guarda datos personales: describe el problema, no copia el valor.
"""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = spark.conf.get("andina.catalog")  # noqa: F821 - `spark` lo inyecta el pipeline
DOUBLE_CHARGE_WINDOW_S = 60  # dos aprobaciones del mismo monto en menos de un minuto = doble clic

# Columnas de la fuente que cada vista *_changes lleva a silver. Si bronze trae una columna que no
# está aquí (cambio de esquema en la fuente), se reporta en silver.unmapped_source_columns.
# Mantener sincronizado con las vistas de silver_clientes, silver_ventas y silver_pagos_tickets.
MAPPED_SOURCE_COLUMNS = {
    "customers": ["CustomerId", "FirstName", "LastName", "Email", "Phone", "City", "Country",
                  "Segment", "SignupDate", "CreatedAt", "UpdatedAt"],
    "products": ["ProductId", "SKU", "Name", "Category", "Subcategory", "Brand", "Price",
                 "Description", "Status", "CreatedAt", "UpdatedAt"],
    "orders": ["OrderId", "CustomerId", "OrderDate", "Channel", "Status", "TotalAmount",
               "CouponCode", "CreatedAt", "UpdatedAt"],
    "order_items": ["OrderItemId", "OrderId", "ProductId", "Quantity", "UnitPrice", "CreatedAt",
                    "UpdatedAt"],
    "payments": ["PaymentId", "OrderId", "Method", "Amount", "Status", "PaymentDate", "CreatedAt",
                 "UpdatedAt"],
    "support_tickets": ["TicketId", "CustomerId", "OrderId", "Channel", "Subject", "Body",
                        "Priority", "Status", "CreatedAt", "UpdatedAt"],
}


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
    comment=f"Pagos que fueron aprobados con el mismo pedido y monto que otro, con menos de "
            f"{DOUBLE_CHARGE_WINDOW_S} s de diferencia. Se detectan desde el historial de estados, así que "
            "siguen visibles después de devolverlos; duplicate_current_status dice si ya se reembolsó.",
)
def payment_double_charges():
    # Cada pago que alguna vez estuvo Aprobado (aunque hoy esté Reembolsado).
    approved = (
        read("silver.payment_status_history")
        .where("status = 'Aprobado'")
        .groupBy("payment_id", "order_id", "amount")
        .agg(F.min("created_at").alias("created_at"))
    )
    current = read("silver.payments").select(
        "payment_id", "method", F.col("status").alias("current_status")
    )
    w = Window.partitionBy("order_id", "amount").orderBy("created_at", "payment_id")
    return (
        approved.join(current, "payment_id")
        .select(
            "order_id", "amount", "method",
            F.col("payment_id").alias("duplicate_payment_id"),
            F.col("created_at").alias("duplicate_created_at"),
            F.col("current_status").alias("duplicate_current_status"),
            F.lag("payment_id").over(w).alias("original_payment_id"),
            F.lag("created_at").over(w).alias("original_created_at"),
        )
        .withColumn(
            "seconds_apart",
            F.col("duplicate_created_at").cast("long") - F.col("original_created_at").cast("long"),
        )
        .where(f"original_payment_id IS NOT NULL AND seconds_apart <= {DOUBLE_CHARGE_WINDOW_S}")
    )


# ---------------------------------------------------------------------------
# Columnas de la fuente que silver no lleva (evolución de esquema)
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="silver.unmapped_source_columns",
    comment="Columnas que existen en bronze pero ninguna vista de silver mapea. Una columna nueva en "
            "la fuente llega sola a bronze; aquí queda visible hasta que se decida llevarla a silver.",
)
def unmapped_source_columns():
    rows = []
    for table, mapped in MAPPED_SOURCE_COLUMNS.items():
        for col in read(f"{CATALOG}.bronze.{table}").columns:
            if not col.startswith("_") and col not in mapped:
                rows.append((table, col))
    return spark.createDataFrame(rows, "bronze_table STRING, column_name STRING")  # noqa: F821


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


def email_problem(col):
    """Describe por qué un email es inválido sin copiar el email (dato personal)."""
    return (
        F.when(~col.contains("@"), F.lit("sin @"))
         .when(col.contains("@@"), F.lit("@ repetida"))
         .when(col.rlike(r"@[^.]+$"), F.lit("dominio sin extensión"))
         .otherwise(F.lit("otro formato"))
    )


@dp.materialized_view(
    name="silver.data_quality_issues",
    comment="Una fila por problema de calidad vigente: regla, acción tomada, entidad e id, sin datos "
            "personales. action: warn (se conserva y se marca), reject (no entra al estado actual), "
            "quarantine (se aparta), info.",
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
    unmapped = read("silver.unmapped_source_columns")

    # Líneas rechazadas que siguen fuera del estado actual (si luego se corrigieron, ya no son problema).
    last = Window.partitionBy("order_item_id").orderBy(F.col("_ct_version").desc())
    rejected = (
        read("silver.rejected_order_items")
        .withColumn("rn", F.row_number().over(last))
        .where("rn = 1")
        .join(items.select("order_item_id"), "order_item_id", "left_anti")
    )
    orders_with_rejected = rejected.select("order_id").distinct().withColumn("_rej", F.lit(True))

    items_total = items.groupBy("order_id").agg(F.sum("line_amount").alias("items_total"))
    # La causa importa para actuar: un total menor que las líneas es un descuento aplicado solo a la
    # cabecera; uno mayor, casi siempre una línea con cantidad 0 que silver excluye.
    with_totals = (
        orders.join(items_total, "order_id")
        .join(orders_with_rejected, "order_id", "left")
        .withColumn(
            "cause",
            F.when(F.col("_rej"), F.lit("línea con cantidad 0 excluida"))
             .when(F.col("total_amount") < F.col("items_total"), F.lit("descuento en la cabecera"))
             .otherwise(F.lit("sin causa conocida")),
        )
    )
    without_lines = (
        orders.join(items.select("order_id").distinct(), "order_id", "left_anti")
        .join(orders_with_rejected, "order_id", "left")
        .withColumn(
            "cause",
            F.when(F.col("_rej"), F.lit("solo tenía líneas con cantidad 0")).otherwise(F.lit("sin líneas en la fuente")),
        )
    )

    parts = [
        issue(customers.where("email IS NOT NULL AND NOT email_valido"),
              "email_formato_invalido", "warn", "customer", "customer_id", email_problem(F.col("email_norm"))),
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
        issue(rejected, "cantidad_no_positiva", "reject", "order_item", "order_item_id",
              F.concat(F.lit("pedido: "), F.col("order_id"), F.lit(" · cantidad: "), F.col("quantity"))),
        issue(quarantine, "producto_inexistente", "quarantine", "order_item", "order_item_id",
              F.concat(F.lit("product_id: "), F.col("product_id"))),
        issue(double, "doble_cobro", "warn", "payment", "duplicate_payment_id",
              F.concat(F.lit("original: "), F.col("original_payment_id"),
                       F.lit(" · segundos: "), F.col("seconds_apart"),
                       F.lit(" · estado actual: "), F.col("duplicate_current_status"))),
        issue(tickets.where("body IS NULL"),
              "ticket_sin_cuerpo", "info", "support_ticket", "ticket_id",
              F.concat(F.lit("canal: "), F.col("channel"))),
        issue(unmapped.withColumn("id", F.concat_ws(".", "bronze_table", "column_name")),
              "columna_no_mapeada", "warn", "schema", "id",
              F.lit("existe en bronze y no llega a silver")),
    ]
    result = parts[0]
    for part in parts[1:]:
        result = result.unionByName(part)
    return result
