# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Modelo de propensión de recompra
# MAGIC
# MAGIC **Pregunta:** dado un cliente que ya compró, ¿volverá a comprar en los próximos 90 días?
# MAGIC
# MAGIC - **Observaciones:** el día 15 de cada mes, cada cliente con al menos una compra efectiva previa.
# MAGIC - **Etiqueta:** 1 si hace una compra efectiva en `(fecha, fecha + 90 días]`. Solo se usan fechas
# MAGIC   cuya ventana de 90 días ya terminó.
# MAGIC - **Features:** `ml.customer_features` con `timestamp_lookup_key`: el feature store trae la foto
# MAGIC   semanal más reciente **anterior** a la observación. Las fotos son los lunes y las observaciones el
# MAGIC   15, a propósito: así se ve que la búsqueda es point-in-time y no un join exacto.
# MAGIC - **Validación temporal:** se entrena con el pasado y se evalúa con meses posteriores. Un split
# MAGIC   aleatorio mezclaría el futuro en el entrenamiento.
# MAGIC - **Promoción con control:** cada versión nueva queda con el alias `challenger`; solo pasa a
# MAGIC   `champion` si su ROC AUC iguala o supera al del champion actual evaluado en el **mismo**
# MAGIC   conjunto de prueba (con `score_batch`). Un reentrenamiento peor nunca reemplaza al modelo en uso.
# MAGIC - **Registro:** MLflow en Unity Catalog (`ml.repurchase_propensity`), alias `champion`. El modelo se
# MAGIC   guarda con `fe.log_model`, que empaqueta qué features usa y de qué tabla: para puntuar basta
# MAGIC   pasar `customer_id` y la fecha.

# COMMAND ----------

# MAGIC %pip install -q databricks-feature-engineering scikit-learn mlflow
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "andina_dev")
catalog = dbutils.widgets.get("catalog")

import mlflow
import pandas as pd
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

fe = FeatureEngineeringClient()
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Shared/andina_market/{catalog}_repurchase_propensity")

MODEL_NAME = f"{catalog}.ml.repurchase_propensity"
FEATURE_TABLE = f"{catalog}.ml.customer_features"
HORIZON_DAYS = 90
EFFECTIVE = ["Pagado", "Enviado", "Entregado"]
NUMERIC = ["tenure_days", "days_since_last_order", "orders_90d", "orders_365d", "orders_lifetime",
           "net_sales_365d", "avg_order_value_365d", "app_share_365d", "tickets_90d",
           "urgent_tickets_90d", "rejected_payments_90d"]
CATEGORICAL = ["favorite_category_365d"]


class ProbabilityModel(mlflow.pyfunc.PythonModel):
    """Envuelve el clasificador para que predict devuelva la probabilidad de la clase positiva."""

    def __init__(self, model):
        self.model = model

    def predict(self, context, model_input):
        # Al puntuar, los enteros pueden llegar como tipos nulables de pandas (Int32 con pd.NA),
        # que sklearn no acepta: se normalizan a float64 (NA → NaN) igual que al entrenar.
        df = model_input.copy()
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]) or pd.api.types.is_bool_dtype(df[col]):
                df[col] = df[col].astype("float64")
        return self.model.predict_proba(df)[:, 1]


def champion_auc_on(test_labels):
    """ROC AUC del champion actual sobre las observaciones de prueba (None si aún no hay champion).

    Usa score_batch, que busca las features con la misma lógica point-in-time que el entrenamiento.
    """
    client = MlflowClient()
    try:
        client.get_model_version_by_alias(MODEL_NAME, "champion")
    except Exception:  # noqa: BLE001 - primer entrenamiento: no hay champion
        return None
    scored = fe.score_batch(model_uri=f"models:/{MODEL_NAME}@champion", df=test_labels)
    pdf = scored.select("label", "prediction").toPandas()
    return float(roc_auc_score(pdf["label"], pdf["prediction"]))

# COMMAND ----------

# MAGIC %md ## Observaciones y etiquetas

# COMMAND ----------

orders = (
    spark.table(f"{catalog}.silver.orders").where(F.col("status").isin(EFFECTIVE))
    .select("customer_id", F.col("order_date").alias("ts"))
)
last_ts = orders.agg(F.max("ts")).first()[0]
first_ts = orders.agg(F.min("ts")).first()[0]
obs_dates = spark.sql(f"""
    SELECT explode(sequence(
        make_timestamp(year(TIMESTAMP'{first_ts}'), month(TIMESTAMP'{first_ts}'), 15, 0, 0, 0) + INTERVAL 3 MONTHS,
        TIMESTAMP'{last_ts}' - INTERVAL {HORIZON_DAYS} DAYS,
        INTERVAL 1 MONTH)) AS obs_ts""")

# Población: clientes con al menos una compra antes de la observación.
population = obs_dates.join(orders, orders.ts < obs_dates.obs_ts).select("customer_id", "obs_ts").distinct()
future = (
    population.join(orders, "customer_id")
    .where((F.col("ts") > F.col("obs_ts")) & (F.col("ts") <= F.col("obs_ts") + F.expr(f"INTERVAL {HORIZON_DAYS} DAYS")))
    .select("customer_id", "obs_ts").distinct().withColumn("label", F.lit(1))
)
labels = (
    population.join(future, ["customer_id", "obs_ts"], "left")
    .select(F.col("customer_id").cast("int"), "obs_ts", F.coalesce("label", F.lit(0)).alias("label"))
)

# COMMAND ----------

# MAGIC %md ## Training set point-in-time desde el feature store

# COMMAND ----------

training_set = fe.create_training_set(
    df=labels,
    feature_lookups=[FeatureLookup(
        table_name=FEATURE_TABLE,
        lookup_key="customer_id",
        timestamp_lookup_key="obs_ts",
        feature_names=NUMERIC + CATEGORICAL,
    )],
    label="label",
)
data = training_set.load_df().toPandas()

# Validación temporal: los últimos 4 meses de observaciones son el conjunto de prueba.
cut = sorted(data["obs_ts"].unique())[-4]
train, test = data[data["obs_ts"] < cut], data[data["obs_ts"] >= cut]
X_cols = NUMERIC + CATEGORICAL
print(f"train={len(train):,} test={len(test):,} corte={cut} tasa_positiva={data['label'].mean():.3f}")

# COMMAND ----------

# MAGIC %md ## Entrenamiento, evaluación y registro

# COMMAND ----------

# Todo el preprocesamiento vive dentro del modelo: al puntuar, score_batch le pasa las features
# crudas y el modelo hace exactamente lo mismo que al entrenar.
model = Pipeline([
    ("prep", ColumnTransformer(
        [("cat", Pipeline([
            ("fill", SimpleImputer(strategy="constant", fill_value="sin_compras")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]), CATEGORICAL)],
        remainder="passthrough",
    )),
    # Las numéricas nulas (cliente sin compras en la ventana) se manejan sin imputar.
    ("clf", HistGradientBoostingClassifier(max_iter=200, learning_rate=0.08, random_state=42)),
])

with mlflow.start_run(run_name="hgb_repurchase") as run:
    model.fit(train[X_cols], train["label"])
    proba = model.predict_proba(test[X_cols])[:, 1]
    # Línea base sin modelo: cuanto más reciente la última compra, más probable la recompra.
    baseline = -test["days_since_last_order"].fillna(10_000)
    metrics = {
        "test_roc_auc": roc_auc_score(test["label"], proba),
        "test_pr_auc": average_precision_score(test["label"], proba),
        "baseline_recency_roc_auc": roc_auc_score(test["label"], baseline),
        "test_positive_rate": float(test["label"].mean()),
        "train_rows": len(train), "test_rows": len(test),
    }
    mlflow.log_params({"horizon_days": HORIZON_DAYS, "split": f"temporal, test desde {cut}",
                       "features": ",".join(X_cols)})
    mlflow.log_metrics(metrics)
    # Se registra la probabilidad, no la clase: la propensión es un puntaje para ordenar clientes.
    fe.log_model(
        model=ProbabilityModel(model),
        artifact_path="model",
        flavor=mlflow.pyfunc,
        training_set=training_set,
        registered_model_name=MODEL_NAME,
        infer_input_example=True,
    )
    print({k: round(v, 4) if isinstance(v, float) else v for k, v in metrics.items()})

    # --- Promoción con control: el champion actual se evalúa en el MISMO conjunto de prueba
    # (comparar contra sus métricas guardadas no sirve: se midieron con otro periodo).
    champion_auc = champion_auc_on(labels.where(F.col("obs_ts") >= F.lit(cut)))
    promoted = champion_auc is None or metrics["test_roc_auc"] >= champion_auc
    mlflow.log_metric("champion_roc_auc_same_test", champion_auc if champion_auc is not None else float("nan"))
    mlflow.log_param("promoted_to_champion", promoted)

client = MlflowClient()
version = max(int(v.version) for v in client.search_model_versions(f"name='{MODEL_NAME}'"))
client.set_registered_model_alias(MODEL_NAME, "challenger", version)
if promoted:
    client.set_registered_model_alias(MODEL_NAME, "champion", version)
    print(f"{MODEL_NAME} v{version} → champion (AUC {metrics['test_roc_auc']:.4f} vs champion anterior {champion_auc})")
else:
    print(f"{MODEL_NAME} v{version} queda como challenger: AUC {metrics['test_roc_auc']:.4f} < champion {champion_auc:.4f}")
