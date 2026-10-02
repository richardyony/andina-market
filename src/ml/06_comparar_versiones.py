# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Comparar dos versiones del modelo de recompra (promoción manual con evidencia)
# MAGIC
# MAGIC La regla automática de promoción compara un número; cuando la diferencia es mínima, una persona
# MAGIC decide con evidencia estadística. Este notebook:
# MAGIC
# MAGIC 1. Arma el **mismo conjunto de prueba para ambas versiones, por persona**: una fila por persona y
# MAGIC    observación (cuenta principal), con la etiqueta calculada sobre las compras de todas sus cuentas,
# MAGIC    en los mismos meses de prueba que usa el entrenamiento.
# MAGIC 2. Puntúa con `score_batch` las dos versiones (mismas features point-in-time).
# MAGIC 3. Calcula el **intervalo de confianza del AUC con bootstrap pareado y agrupado por persona**: se
# MAGIC    remuestrean personas, no filas, porque cada persona aparece en varios meses y sus filas no son
# MAGIC    independientes. En cada remuestreo se calcula el AUC de ambas versiones y su diferencia.
# MAGIC 4. **Equivalencia:** el IC 95 % de la diferencia (B − A) contiene 0 y está dentro de ±`margin`.
# MAGIC 5. Si `apply = true` y son equivalentes: `champion` → versión B, `previous` → versión A (para
# MAGIC    revertir) y tags de auditoría en la versión B. Si no son equivalentes, no cambia nada.

# COMMAND ----------

# MAGIC %pip install -q databricks-feature-engineering scikit-learn mlflow
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "andina_dev")
dbutils.widgets.text("version_a", "2")      # champion actual
dbutils.widgets.text("version_b", "4")      # candidata
dbutils.widgets.text("n_boot", "1000")
dbutils.widgets.text("margin", "0.01")      # diferencia de AUC considerada irrelevante
dbutils.widgets.dropdown("apply", "false", ["false", "true"])
dbutils.widgets.text("motivo", "")
dbutils.widgets.text("aprobado_por", "")

catalog = dbutils.widgets.get("catalog")
va, vb = dbutils.widgets.get("version_a"), dbutils.widgets.get("version_b")
n_boot, margin = int(dbutils.widgets.get("n_boot")), float(dbutils.widgets.get("margin"))
apply = dbutils.widgets.get("apply") == "true"

import json
from datetime import datetime, timezone

import mlflow
import numpy as np
from databricks.feature_engineering import FeatureEngineeringClient
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F
from sklearn.metrics import roc_auc_score

fe = FeatureEngineeringClient()
mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()
MODEL = f"{catalog}.ml.repurchase_propensity"
EFFECTIVE = ["Pagado", "Enviado", "Entregado"]
HORIZON_DAYS = 90

# COMMAND ----------

# MAGIC %md ## 1. Conjunto de prueba por persona (misma construcción que el entrenamiento)

# COMMAND ----------

s = f"{catalog}.silver"
person = (
    spark.table(f"{s}.customers").select("customer_id")
    .join(spark.table(f"{s}.customer_duplicates").select("customer_id", "principal_customer_id"), "customer_id", "left")
    .select("customer_id", F.coalesce("principal_customer_id", "customer_id").alias("person_id"))
)
orders = (
    spark.table(f"{s}.orders").where(F.col("status").isin(EFFECTIVE))
    .select("customer_id", F.col("order_date").alias("ts"))
    .join(person, "customer_id").select(F.col("person_id").alias("customer_id"), "ts")
)
last_ts, first_ts = orders.agg(F.max("ts"), F.min("ts")).first()
obs_dates = spark.sql(f"""
    SELECT explode(sequence(
        make_timestamp(year(TIMESTAMP'{first_ts}'), month(TIMESTAMP'{first_ts}'), 15, 0, 0, 0) + INTERVAL 3 MONTHS,
        TIMESTAMP'{last_ts}' - INTERVAL {HORIZON_DAYS} DAYS, INTERVAL 1 MONTH)) AS obs_ts""")
cut = sorted(r[0] for r in obs_dates.collect())[-4]          # los mismos 4 meses de prueba del entrenamiento
obs_test = obs_dates.where(F.col("obs_ts") >= F.lit(cut))
population = obs_test.join(orders, orders.ts < obs_test.obs_ts).select("customer_id", "obs_ts").distinct()
future = (
    population.join(orders, "customer_id")
    .where((F.col("ts") > F.col("obs_ts")) & (F.col("ts") <= F.col("obs_ts") + F.expr(f"INTERVAL {HORIZON_DAYS} DAYS")))
    .select("customer_id", "obs_ts").distinct().withColumn("label", F.lit(1))
)
test = (
    population.join(future, ["customer_id", "obs_ts"], "left")
    .select(F.col("customer_id").cast("int"), "obs_ts", F.coalesce("label", F.lit(0)).alias("label"))
)

# COMMAND ----------

# MAGIC %md ## 2. Puntuar las dos versiones sobre las mismas filas

# COMMAND ----------

def score(version):
    return (fe.score_batch(model_uri=f"models:/{MODEL}/{version}", df=test)
            .select("customer_id", "obs_ts", "label", F.col("prediction").alias(f"p{version}")))

pdf = score(va).join(score(vb).drop("label"), ["customer_id", "obs_ts"]).toPandas()
y, pa, pb = pdf["label"].to_numpy(), pdf[f"p{va}"].to_numpy(), pdf[f"p{vb}"].to_numpy()
auc_a, auc_b = roc_auc_score(y, pa), roc_auc_score(y, pb)
print(f"Prueba desde {cut}: {len(pdf):,} filas, {pdf['customer_id'].nunique():,} personas, tasa positiva {y.mean():.3f}")
print(f"AUC v{va} = {auc_a:.4f} · AUC v{vb} = {auc_b:.4f} · diferencia = {auc_b - auc_a:+.4f}")

# COMMAND ----------

# MAGIC %md ## 3. Bootstrap pareado, agrupado por persona

# COMMAND ----------

rng = np.random.default_rng(42)
persons = pdf["customer_id"].to_numpy()
uniq, inv = np.unique(persons, return_inverse=True)
rows_by_person = [np.flatnonzero(inv == i) for i in range(len(uniq))]
boot_a, boot_b, boot_d = [], [], []
for _ in range(n_boot):
    pick = rng.integers(0, len(uniq), len(uniq))                 # personas con reemplazo
    idx = np.concatenate([rows_by_person[i] for i in pick])
    if y[idx].min() == y[idx].max():
        continue                                                  # AUC indefinido con una sola clase
    a, b = roc_auc_score(y[idx], pa[idx]), roc_auc_score(y[idx], pb[idx])
    boot_a.append(a); boot_b.append(b); boot_d.append(b - a)

ci = lambda v: (float(np.percentile(v, 2.5)), float(np.percentile(v, 97.5)))
ci_a, ci_b, ci_d = ci(boot_a), ci(boot_b), ci(boot_d)
equivalent = ci_d[0] <= 0 <= ci_d[1] and -margin <= ci_d[0] and ci_d[1] <= margin
print(f"IC 95 % AUC v{va}: [{ci_a[0]:.4f}, {ci_a[1]:.4f}]")
print(f"IC 95 % AUC v{vb}: [{ci_b[0]:.4f}, {ci_b[1]:.4f}]")
print(f"IC 95 % diferencia (v{vb} − v{va}): [{ci_d[0]:+.4f}, {ci_d[1]:+.4f}] · margen ±{margin}")
print("EQUIVALENTES" if equivalent else "NO equivalentes: no se cambia nada")

# COMMAND ----------

# MAGIC %md ## 4. Registro y, si corresponde, promoción con trazabilidad

# COMMAND ----------

result = {
    "model": MODEL, "version_a": int(va), "version_b": int(vb), "test_from": str(cut),
    "rows": int(len(pdf)), "persons": int(len(uniq)), "positive_rate": float(y.mean()),
    "auc_a": float(auc_a), "auc_b": float(auc_b), "auc_diff": float(auc_b - auc_a),
    "ci_a": ci_a, "ci_b": ci_b, "ci_diff": ci_d, "n_boot": len(boot_d), "margin": margin,
    "equivalent": bool(equivalent),
}
mlflow.set_experiment(f"/Shared/andina_market/{catalog}_repurchase_propensity")
with mlflow.start_run(run_name=f"comparacion_v{va}_vs_v{vb}"):
    mlflow.log_params({k: v for k, v in result.items() if not isinstance(v, (tuple, float))})
    mlflow.log_metrics({"auc_a": auc_a, "auc_b": auc_b, "auc_diff": auc_b - auc_a,
                        "ci_diff_low": ci_d[0], "ci_diff_high": ci_d[1]})
    mlflow.log_dict(result, "comparacion.json")

applied = False
if apply and equivalent:
    motivo = dbutils.widgets.get("motivo")
    aprobado = dbutils.widgets.get("aprobado_por")
    client.set_registered_model_alias(MODEL, "previous", va)       # para revertir: mover champion a @previous
    client.set_registered_model_alias(MODEL, "champion", vb)
    try:
        client.delete_registered_model_alias(MODEL, "challenger")  # la candidata ya es champion
    except Exception:  # noqa: BLE001
        pass
    for k, v in {"promocion": "manual", "motivo": motivo, "aprobado_por": aprobado,
                 "evidencia": f"AUC v{vb}-v{va} {auc_b - auc_a:+.4f}, IC95 [{ci_d[0]:+.4f}, {ci_d[1]:+.4f}], "
                              f"{len(uniq)} personas, bootstrap por persona B={len(boot_d)}",
                 "promovido_en": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")}.items():
        client.set_model_version_tag(MODEL, vb, k, v)
    applied = True

raw = client.get_registered_model(MODEL).aliases or {}
aliases = dict(raw) if isinstance(raw, dict) else {a.alias: a.version for a in raw}
print(json.dumps({**result, "applied": applied, "aliases": aliases}, indent=1, default=str))
dbutils.notebook.exit(json.dumps({**result, "applied": applied, "aliases": aliases}, default=str))
