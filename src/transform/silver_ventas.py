"""Silver · catálogo y ventas: productos, pedidos y líneas de pedido (SCD1).

Cada entidad sigue el mismo patrón: vista con la normalización y las expectations,
y AUTO CDC hacia el estado actual, ordenado por `_ct_version` y aplicando los DELETE.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("andina.catalog")  # noqa: F821 - `spark` lo inyecta el pipeline
LINEAGE = ["_ct_version", "_ct_operation", "_batch_id", "_ingested_at"]


def scd1(target: str, source: str, key: str, comment: str):
    """Tabla de estado actual: la última versión por clave; un DELETE en la fuente la borra."""
    dp.create_streaming_table(name=target, comment=comment, cluster_by=[key])
    dp.create_auto_cdc_flow(
        target=target,
        source=source,
        keys=[key],
        sequence_by=F.col("_ct_version"),
        apply_as_deletes=F.expr("_ct_operation = 'D'"),
        except_column_list=["_ct_operation"],
        stored_as_scd_type=1,
    )


# ---------------------------------------------------------------------------
# Productos
# ---------------------------------------------------------------------------
@dp.temporary_view(comment="Cambios de productos normalizados")
@dp.expect_or_fail("pk_presente", "product_id IS NOT NULL")
@dp.expect("precio_positivo", "_ct_operation = 'D' OR price > 0")
@dp.expect("estado_conocido", "_ct_operation = 'D' OR status IN ('Active','Discontinued')")
def products_changes():
    return spark.readStream.table(f"{CATALOG}.bronze.products").select(  # noqa: F821
        F.col("ProductId").alias("product_id"),
        F.trim("SKU").alias("sku"),
        F.trim("Name").alias("product_name"),
        F.col("Category").alias("category"),
        F.col("Subcategory").alias("subcategory"),
        F.col("Brand").alias("brand"),
        F.col("Price").alias("price"),
        F.col("Description").alias("description"),
        F.col("Status").alias("status"),
        (F.col("Status") == "Active").alias("is_active"),
        F.col("CreatedAt").alias("created_at"),
        F.col("UpdatedAt").alias("updated_at"),
        *LINEAGE,
    )


scd1("silver.products", "products_changes", "product_id",
     "Catálogo, estado actual (SCD1). Los descontinuados se conservan con is_active = false.")


# ---------------------------------------------------------------------------
# Pedidos
# ---------------------------------------------------------------------------
@dp.temporary_view(comment="Cambios de pedidos normalizados")
@dp.expect_or_fail("pk_presente", "order_id IS NOT NULL")
@dp.expect("fecha_no_futura", "_ct_operation = 'D' OR NOT order_date_corrected")
@dp.expect("canal_conocido", "_ct_operation = 'D' OR channel IN ('web','app','tienda')")
@dp.expect("total_no_negativo", "_ct_operation = 'D' OR total_amount >= 0")
def orders_changes():
    df = spark.readStream.table(f"{CATALOG}.bronze.orders")  # noqa: F821
    # Un OrderDate más de un día posterior a la creación del registro es un error de captura
    # (año mal digitado): se usa CreatedAt, que pone la base y es confiable. Se conserva el original.
    corrected = F.col("OrderDate") > F.col("CreatedAt") + F.expr("INTERVAL 1 DAY")
    # CouponCode llegó con un cambio de esquema: si la columna aún no existe (p. ej. en un
    # entorno sin ese cambio), el pipeline sigue funcionando con NULL.
    coupon = F.col("CouponCode") if "CouponCode" in df.columns else F.lit(None).cast("string")
    return df.select(
        F.col("OrderId").alias("order_id"),
        F.col("CustomerId").alias("customer_id"),
        F.col("OrderDate").alias("order_date_raw"),
        F.when(corrected, F.col("CreatedAt")).otherwise(F.col("OrderDate")).alias("order_date"),
        F.coalesce(corrected, F.lit(False)).alias("order_date_corrected"),
        F.lower(F.trim("Channel")).alias("channel"),
        F.col("Status").alias("status"),
        F.col("TotalAmount").alias("total_amount"),
        coupon.alias("coupon_code"),
        F.col("CreatedAt").alias("created_at"),
        F.col("UpdatedAt").alias("updated_at"),
        *LINEAGE,
    )


scd1("silver.orders", "orders_changes", "order_id",
     "Pedidos, estado actual (SCD1). order_date corregida si el año venía mal digitado.")


# ---------------------------------------------------------------------------
# Líneas de pedido
# ---------------------------------------------------------------------------
@dp.temporary_view(comment="Cambios de líneas de pedido normalizados")
@dp.expect_or_fail("pk_presente", "order_item_id IS NOT NULL")
# Cantidad 0 = error de captura: la línea no entra a silver. No es silencioso: el conteo queda
# en las métricas del pipeline y cada caso se lista en silver.data_quality_issues.
@dp.expect_or_drop("cantidad_positiva", "_ct_operation = 'D' OR quantity > 0")
@dp.expect("precio_no_negativo", "_ct_operation = 'D' OR unit_price >= 0")
def order_items_changes():
    return spark.readStream.table(f"{CATALOG}.bronze.order_items").select(  # noqa: F821
        F.col("OrderItemId").alias("order_item_id"),
        F.col("OrderId").alias("order_id"),
        F.col("ProductId").alias("product_id"),
        F.col("Quantity").alias("quantity"),
        F.col("UnitPrice").alias("unit_price"),
        (F.col("Quantity") * F.col("UnitPrice")).cast("decimal(14,2)").alias("line_amount"),
        F.col("CreatedAt").alias("created_at"),
        F.col("UpdatedAt").alias("updated_at"),
        *LINEAGE,
    )


scd1("silver.order_items", "order_items_changes", "order_item_id",
     "Líneas de pedido, estado actual (SCD1). Sin cantidades 0. Los ProductId huérfanos se conservan "
     "y se listan en silver.quarantine_order_items.")
