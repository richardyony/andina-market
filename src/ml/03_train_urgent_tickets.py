# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Modelo de tickets urgentes
# MAGIC
# MAGIC **Pregunta:** al llegar un ticket, ¿es urgente? Hoy un agente asigna la prioridad a mano; el
# MAGIC modelo permite ordenar la cola antes de que alguien lo lea.
# MAGIC
# MAGIC - **Etiqueta:** `priority = 'Urgente'` asignada por el agente. Tiene ruido: en los datos de
# MAGIC   origen, ~8 % de las prioridades están mal etiquetadas (`docs/datos_sinteticos.md`), así que el
# MAGIC   techo de cualquier modelo está por debajo de la perfección.
# MAGIC - **Features de dos tablas del feature store:**
# MAGIC   - `ml.ticket_features` por `ticket_id`: asunto, cuerpo, canal, largo, si tiene pedido, hora;
# MAGIC   - `ml.customer_features` con `timestamp_lookup_key = created_at`: el historial del cliente
# MAGIC     **antes** del ticket (pedidos, tickets y pagos rechazados recientes). La foto es anterior al
# MAGIC     ticket, así que no incluye el ticket mismo.
# MAGIC - **Validación temporal:** el 20 % más reciente de los tickets es el conjunto de prueba.

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
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, precision_score, recall_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

fe = FeatureEngineeringClient()
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Shared/andina_market/{catalog}_urgent_tickets")

MODEL_NAME = f"{catalog}.ml.urgent_ticket_classifier"
TICKET_FEATURES = ["subject", "body", "channel", "body_length", "has_order", "created_hour"]
CUSTOMER_FEATURES = ["orders_90d", "tickets_90d", "urgent_tickets_90d", "rejected_payments_90d",
                     "days_since_last_order"]
NUMERIC = ["body_length", "created_hour"] + CUSTOMER_FEATURES


class ProbabilityModel(mlflow.pyfunc.PythonModel):
    """Devuelve la probabilidad de que el ticket sea urgente."""

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

# COMMAND ----------

labels = spark.table(f"{catalog}.silver.support_tickets").select(
    F.col("ticket_id").cast("int"),
    F.col("customer_id").cast("int"),
    "created_at",
    (F.col("priority") == "Urgente").cast("int").alias("label"),
)

training_set = fe.create_training_set(
    df=labels,
    feature_lookups=[
        FeatureLookup(table_name=f"{catalog}.ml.ticket_features", lookup_key="ticket_id",
                      feature_names=TICKET_FEATURES),
        FeatureLookup(table_name=f"{catalog}.ml.customer_features", lookup_key="customer_id",
                      timestamp_lookup_key="created_at", feature_names=CUSTOMER_FEATURES),
    ],
    label="label",
    exclude_columns=["customer_id"],
)
data = training_set.load_df().toPandas().sort_values("created_at")
cut = int(len(data) * 0.8)
train, test = data.iloc[:cut], data.iloc[cut:]
X_cols = ["subject", "body", "channel", "has_order"] + NUMERIC
print(f"train={len(train):,} test={len(test):,} tasa_urgente={data['label'].mean():.3f}")

# COMMAND ----------

model = Pipeline([
    ("prep", ColumnTransformer([
        ("subject", TfidfVectorizer(ngram_range=(1, 2), min_df=2, strip_accents="unicode"), "subject"),
        ("body", TfidfVectorizer(ngram_range=(1, 2), min_df=3, max_features=5000, strip_accents="unicode"), "body"),
        ("channel", OneHotEncoder(handle_unknown="ignore"), ["channel"]),
        ("num", Pipeline([("fill", SimpleImputer(strategy="constant", fill_value=0)),
                          ("scale", StandardScaler())]), NUMERIC + ["has_order"]),
    ])),
    # class_weight: las urgentes son minoría; sin balancear, el modelo aprendería a no marcarlas.
    ("clf", LogisticRegression(max_iter=2000, class_weight="balanced", C=2.0)),
])

with mlflow.start_run(run_name="tfidf_logreg_urgent"):
    model.fit(train[X_cols], train["label"])
    proba = model.predict_proba(test[X_cols])[:, 1]
    pred = (proba >= 0.5).astype(int)
    metrics = {
        "test_roc_auc": roc_auc_score(test["label"], proba),
        "test_pr_auc": average_precision_score(test["label"], proba),
        "test_recall_urgente": recall_score(test["label"], pred),
        "test_precision_urgente": precision_score(test["label"], pred),
        "test_positive_rate": float(test["label"].mean()),
        "train_rows": len(train), "test_rows": len(test),
    }
    mlflow.log_params({"split": "temporal, 20 % más reciente", "features": ",".join(X_cols)})
    mlflow.log_metrics(metrics)
    fe.log_model(
        model=ProbabilityModel(model),
        artifact_path="model",
        flavor=mlflow.pyfunc,
        training_set=training_set,
        registered_model_name=MODEL_NAME,
        infer_input_example=True,
    )
    print({k: round(v, 4) if isinstance(v, float) else v for k, v in metrics.items()})

client = MlflowClient()
version = max(int(v.version) for v in client.search_model_versions(f"name='{MODEL_NAME}'"))
client.set_registered_model_alias(MODEL_NAME, "champion", version)
print(f"{MODEL_NAME} v{version} → alias champion")
