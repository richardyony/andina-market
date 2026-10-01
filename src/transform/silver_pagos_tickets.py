"""Silver · pagos y tickets de soporte.

Pagos: estado actual (SCD1) e historial de estados (SCD2), porque un pago cambia después
de creado (Pendiente → Aprobado / Rechazado → Reembolsado) y ese recorrido importa.
Tickets: estado actual (SCD1); los DELETE de la fuente (spam) se aplican.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("andina.catalog")  # noqa: F821 - `spark` lo inyecta el pipeline
LINEAGE = ["_ct_version", "_ct_operation", "_batch_id", "_ingested_at"]


# ---------------------------------------------------------------------------
# Pagos
# ---------------------------------------------------------------------------
@dp.temporary_view(comment="Cambios de pagos normalizados")
@dp.expect_or_fail("pk_presente", "payment_id IS NOT NULL")
@dp.expect("monto_positivo", "_ct_operation = 'D' OR amount > 0")
@dp.expect("estado_conocido", "_ct_operation = 'D' OR status IN ('Pendiente','Aprobado','Rechazado','Reembolsado')")
def payments_changes():
    return spark.readStream.table(f"{CATALOG}.bronze.payments").select(  # noqa: F821
        F.col("PaymentId").alias("payment_id"),
        F.col("OrderId").alias("order_id"),
        F.lower(F.trim("Method")).alias("method"),
        F.col("Amount").alias("amount"),
        F.col("Status").alias("status"),
        F.col("PaymentDate").alias("payment_date"),
        F.col("CreatedAt").alias("created_at"),
        F.col("UpdatedAt").alias("updated_at"),
        *LINEAGE,
    )


dp.create_streaming_table(
    name="silver.payments",
    comment="Pagos, estado actual (SCD1).",
    cluster_by=["payment_id"],
)
dp.create_auto_cdc_flow(
    target="silver.payments",
    source="payments_changes",
    keys=["payment_id"],
    sequence_by=F.col("_ct_version"),
    apply_as_deletes=F.expr("_ct_operation = 'D'"),
    except_column_list=["_ct_operation"],
    stored_as_scd_type=1,
)

dp.create_streaming_table(
    name="silver.payment_status_history",
    comment="Historial SCD2 del estado de cada pago. Vigencia en __START_AT/__END_AT (versión CT y updated_at).",
    cluster_by=["payment_id"],
)
dp.create_auto_cdc_flow(
    target="silver.payment_status_history",
    source="payments_changes",
    keys=["payment_id"],
    sequence_by=F.struct("_ct_version", "updated_at"),
    apply_as_deletes=F.expr("_ct_operation = 'D'"),
    column_list=["payment_id", "order_id", "status", "amount", "created_at", "updated_at"],
    stored_as_scd_type=2,
    track_history_column_list=["status"],
)


# ---------------------------------------------------------------------------
# Tickets de soporte
# ---------------------------------------------------------------------------
@dp.temporary_view(comment="Cambios de tickets normalizados")
@dp.expect_or_fail("pk_presente", "ticket_id IS NOT NULL")
@dp.expect("prioridad_conocida", "_ct_operation = 'D' OR priority IN ('Baja','Media','Alta','Urgente')")
@dp.expect("cuerpo_presente", "_ct_operation = 'D' OR body IS NOT NULL")
def support_tickets_changes():
    return spark.readStream.table(f"{CATALOG}.bronze.support_tickets").select(  # noqa: F821
        F.col("TicketId").alias("ticket_id"),
        F.col("CustomerId").alias("customer_id"),
        F.col("OrderId").alias("order_id"),
        F.lower(F.trim("Channel")).alias("channel"),
        F.trim("Subject").alias("subject"),
        # '' y solo espacios son "sin cuerpo": se normalizan a NULL.
        F.nullif(F.trim("Body"), F.lit("")).alias("body"),
        F.col("Priority").alias("priority"),
        F.col("Status").alias("status"),
        F.col("CreatedAt").alias("created_at"),
        F.col("UpdatedAt").alias("updated_at"),
        *LINEAGE,
    )


dp.create_streaming_table(
    name="silver.support_tickets",
    comment="Tickets de soporte, estado actual (SCD1). Cuerpo vacío normalizado a NULL; spam borrado en la fuente se elimina.",
    cluster_by=["ticket_id"],
)
dp.create_auto_cdc_flow(
    target="silver.support_tickets",
    source="support_tickets_changes",
    keys=["ticket_id"],
    sequence_by=F.col("_ct_version"),
    apply_as_deletes=F.expr("_ct_operation = 'D'"),
    except_column_list=["_ct_operation"],
    stored_as_scd_type=1,
)
