# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Validación del lakehouse
# MAGIC
# MAGIC Última tarea del job. Comprueba las garantías de punta a punta y **falla** si alguna no se
# MAGIC cumple, para que el job lo muestre y avise por correo. Cada corrida deja su resultado en
# MAGIC `ops.validation_log`.
# MAGIC
# MAGIC | Garantía | Cómo se comprueba |
# MAGIC |---|---|
# MAGIC | La ingesta no pierde ni duplica cambios | Estado actual reconstruido desde bronze = silver, por tabla |
# MAGIC | Silver tiene una fila por clave | Sin claves duplicadas |
# MAGIC | Gold cuadra con silver | Mismas ventas, líneas, pedidos y pagos |
# MAGIC | El modelo estrella es íntegro | Ninguna fila de hechos sin cliente, fecha o producto |
# MAGIC | El calendario cubre los hechos | Toda `date_key` existe en `dim_date` |

# COMMAND ----------

dbutils.widgets.text("catalog", "andina_dev")
dbutils.widgets.text("batch_id", "")

# COMMAND ----------

from datetime import datetime, timezone

catalog = dbutils.widgets.get("catalog")
batch_id = dbutils.widgets.get("batch_id") or "manual_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def value(sql: str):
    return spark.sql(sql).collect()[0][0]


# (tabla bronze, clave en bronze, tabla silver, clave en silver, condición extra para "vigente")
ENTITIES = [
    ("customers", "CustomerId", "customers", "customer_id", ""),
    ("products", "ProductId", "products", "product_id", ""),
    ("orders", "OrderId", "orders", "order_id", ""),
    # Las líneas con cantidad <= 0 se excluyen del estado actual a propósito (D-14).
    ("order_items", "OrderItemId", "order_items", "order_item_id", "AND Quantity > 0"),
    ("payments", "PaymentId", "payments", "payment_id", ""),
    ("support_tickets", "TicketId", "support_tickets", "ticket_id", ""),
]

checks = []  # (nombre, esperado, obtenido)

for bronze, bpk, silver, spk, extra in ENTITIES:
    expected = value(f"""
        SELECT count(*) FROM (
            SELECT *, row_number() OVER (PARTITION BY {bpk} ORDER BY _ct_version DESC) AS rn
            FROM {catalog}.bronze.{bronze})
        WHERE rn = 1 AND _ct_operation <> 'D' {extra}""")
    got = value(f"SELECT count(*) FROM {catalog}.silver.{silver}")
    checks.append((f"bronze_vs_silver.{silver}", expected, got))
    checks.append((f"claves_unicas.{silver}", 0,
                   value(f"SELECT count(*) - count(DISTINCT {spk}) FROM {catalog}.silver.{silver}")))

g, s = f"{catalog}.gold", f"{catalog}.silver"
checks += [
    ("gold_vs_silver.ventas", value(f"SELECT sum(line_amount) FROM {s}.order_items"),
     value(f"SELECT sum(line_amount) FROM {g}.fact_order_lines")),
    ("gold_vs_silver.lineas", value(f"SELECT count(*) FROM {s}.order_items"),
     value(f"SELECT count(*) FROM {g}.fact_order_lines")),
    ("gold_vs_silver.pedidos", value(f"SELECT count(*) FROM {s}.orders"),
     value(f"SELECT count(*) FROM {g}.fact_orders")),
    ("gold_vs_silver.pagos", value(f"SELECT count(*) FROM {s}.payments"),
     value(f"SELECT count(*) FROM {g}.fact_payments")),
    ("integridad.lineas_sin_dimension", 0, value(
        f"SELECT count(*) FROM {g}.fact_order_lines WHERE customer_key IS NULL OR product_key IS NULL OR date_key IS NULL")),
    ("integridad.pedidos_sin_dimension", 0, value(
        f"SELECT count(*) FROM {g}.fact_orders WHERE customer_key IS NULL OR date_key IS NULL")),
    ("integridad.pagos_sin_dimension", 0, value(
        f"SELECT count(*) FROM {g}.fact_payments WHERE customer_key IS NULL OR date_key IS NULL")),
    ("integridad.fechas_fuera_de_calendario", 0, value(f"""
        SELECT count(*) FROM (
            SELECT date_key FROM {g}.fact_orders UNION ALL SELECT date_key FROM {g}.fact_payments)
        WHERE date_key NOT IN (SELECT date_key FROM {g}.dim_date)""")),
    ("integridad.clientes_con_una_version_vigente", 0, value(f"""
        SELECT count(*) FROM (SELECT customer_id FROM {g}.dim_customer
                              GROUP BY customer_id HAVING sum(CAST(is_current AS INT)) <> 1)""")),
]

# COMMAND ----------

now = datetime.now(timezone.utc)
rows = [
    (batch_id, name, str(expected), str(got), expected == got, now)
    for name, expected, got in checks
]
result = spark.createDataFrame(
    rows, "batch_id STRING, check_name STRING, expected STRING, actual STRING, passed BOOLEAN, checked_at TIMESTAMP"
)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.ops.validation_log (
    batch_id STRING, check_name STRING, expected STRING, actual STRING, passed BOOLEAN, checked_at TIMESTAMP
) COMMENT 'Resultado de cada comprobación de punta a punta, por corrida del job'
""")
result.write.mode("append").saveAsTable(f"{catalog}.ops.validation_log")

for name, expected, got in checks:
    print(f"{'OK   ' if expected == got else 'FALLA'} {name:<45} esperado={expected} obtenido={got}")

failed = [name for name, expected, got in checks if expected != got]
if failed:
    raise AssertionError(f"Validaciones fallidas: {failed}")
print(f"\n{len(checks)} validaciones correctas")
