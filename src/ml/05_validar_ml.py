# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Validación de la capa de ML
# MAGIC
# MAGIC Última tarea del job `andina_ml`. Comprueba las garantías del feature store y de los modelos y
# MAGIC **falla** si alguna no se cumple. Deja el resultado en `ops.validation_log`, junto con las
# MAGIC validaciones del lakehouse.
# MAGIC
# MAGIC | Garantía | Comprobación |
# MAGIC |---|---|
# MAGIC | Point-in-time | En 3 fotos, `orders_lifetime` = compras efectivas **anteriores** a la fecha de la foto, fila por fila |
# MAGIC | Sin fotos imposibles | Ninguna foto anterior al alta del cliente |
# MAGIC | Claves únicas | Una fila por clave primaria en cada tabla de features |
# MAGIC | Frescura | La última foto no tiene más de 7 días de atraso respecto del último pedido |
# MAGIC | Modelos | Cada modelo tiene `champion`; el de recompra supera a la regla de recencia |
# MAGIC | Puntajes | Probabilidades entre 0 y 1; un puntaje por cliente con compras |

# COMMAND ----------

dbutils.widgets.text("catalog", "andina_dev")
dbutils.widgets.text("batch_id", "")

# COMMAND ----------

from datetime import datetime, timezone

from mlflow.tracking import MlflowClient
import mlflow

catalog = dbutils.widgets.get("catalog")
batch_id = dbutils.widgets.get("batch_id") or "manual_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
mlflow.set_registry_uri("databricks-uc")
ml, s = f"{catalog}.ml", f"{catalog}.silver"


def value(sql):
    return spark.sql(sql).collect()[0][0]


checks = []

# Point-in-time: tres fotos repartidas en el historial (la primera con datos, la del medio, la última).
snaps = [r[0] for r in spark.sql(f"SELECT DISTINCT as_of_ts FROM {ml}.customer_features ORDER BY 1").collect()]
for snap in [snaps[len(snaps) // 4], snaps[len(snaps) // 2], snaps[-1]]:
    mismatches = value(f"""
        SELECT count(*) FROM {ml}.customer_features f
        LEFT JOIN (
            SELECT f2.customer_id, count(o.order_id) AS n
            FROM {ml}.customer_features f2
            LEFT JOIN {s}.orders o
              ON o.customer_id = f2.customer_id
             AND o.status IN ('Pagado','Enviado','Entregado')
             AND o.order_date < f2.as_of_ts
            WHERE f2.as_of_ts = TIMESTAMP'{snap}'
            GROUP BY f2.customer_id) real ON real.customer_id = f.customer_id
        WHERE f.as_of_ts = TIMESTAMP'{snap}' AND f.orders_lifetime <> real.n""")
    checks.append((f"ml.point_in_time.{str(snap)[:10]}", 0, mismatches))

checks += [
    ("ml.fotos_antes_del_alta", 0, value(f"""
        SELECT count(*) FROM {ml}.customer_features f JOIN {s}.customers c USING (customer_id)
        WHERE to_date(f.as_of_ts) <= c.signup_date""")),
    ("ml.claves_unicas.customer_features", 0, value(
        f"SELECT count(*) - count(DISTINCT customer_id, as_of_ts) FROM {ml}.customer_features")),
    ("ml.claves_unicas.ticket_features", 0, value(
        f"SELECT count(*) - count(DISTINCT ticket_id) FROM {ml}.ticket_features")),
    ("ml.frescura_fotos_dias_de_atraso_mayor_a_7", False, value(f"""
        SELECT datediff((SELECT max(order_date) FROM {s}.orders), (SELECT max(as_of_ts) FROM {ml}.customer_features)) > 7""")),
    ("ml.puntajes_fuera_de_rango", 0, value(f"""
        SELECT (SELECT count(*) FROM {ml}.repurchase_scores WHERE repurchase_probability NOT BETWEEN 0 AND 1)
             + (SELECT count(*) FROM {ml}.urgent_ticket_scores WHERE urgency_probability NOT BETWEEN 0 AND 1)""")),
    ("ml.clientes_puntuados",
     value(f"""SELECT count(*) FROM {ml}.customer_features
               WHERE as_of_ts = (SELECT max(as_of_ts) FROM {ml}.customer_features) AND orders_lifetime > 0"""),
     value(f"SELECT count(*) FROM {ml}.repurchase_scores")),
]

# Modelos: alias champion y, para recompra, mejor que la línea base sin modelo.
client = MlflowClient()
for model in [f"{ml}.repurchase_propensity", f"{ml}.urgent_ticket_classifier"]:
    try:
        mv = client.get_model_version_by_alias(model, "champion")
        has_champion = True
    except Exception:  # noqa: BLE001
        mv, has_champion = None, False
    checks.append((f"ml.tiene_champion.{model.split('.')[-1]}", True, has_champion))
    if mv is not None and model.endswith("repurchase_propensity"):
        m = client.get_run(mv.run_id).data.metrics
        checks.append(("ml.champion_supera_linea_base", True,
                       m.get("test_roc_auc", 0) > m.get("baseline_recency_roc_auc", 1)))

# COMMAND ----------

now = datetime.now(timezone.utc)
rows = [(batch_id, name, str(exp), str(got), exp == got, now) for name, exp, got in checks]
spark.createDataFrame(
    rows, "batch_id STRING, check_name STRING, expected STRING, actual STRING, passed BOOLEAN, checked_at TIMESTAMP"
).write.mode("append").saveAsTable(f"{catalog}.ops.validation_log")

for name, exp, got in checks:
    print(f"{'OK   ' if exp == got else 'FALLA'} {name:<50} esperado={exp} obtenido={got}")
failed = [name for name, exp, got in checks if exp != got]
if failed:
    raise AssertionError(f"Validaciones de ML fallidas: {failed}")
print(f"\n{len(checks)} validaciones de ML correctas")
