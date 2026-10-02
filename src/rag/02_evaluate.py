# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Evaluación del retrieval y consultas de ejemplo
# MAGIC
# MAGIC **No basta con que "responda algo":** se mide si lo recuperado es lo correcto.
# MAGIC
# MAGIC - **Golden set** (`golden_set.json`): 30 preguntas redactadas como las haría un cliente, cada una
# MAGIC   con la sección o el producto que la responde (a veces más de uno válido).
# MAGIC - **Métricas:** recall@k (¿aparece un chunk relevante entre los k primeros?) para k = 1, 3, 5, y
# MAGIC   MRR (qué tan arriba aparece el primero relevante).
# MAGIC - **Dos modos de búsqueda:** `ANN` (solo vectorial) e `HYBRID` (vectorial + palabras clave). Las
# MAGIC   preguntas de producto traen nombres propios y medidas ("aro 29", "2.8 litros") donde las palabras
# MAGIC   exactas ayudan.
# MAGIC - **Consultas de ejemplo:** 5 preguntas con lo recuperado y una respuesta generada que cita sus
# MAGIC   fuentes (documento y sección), para la trazabilidad de cada respuesta.
# MAGIC
# MAGIC Resultados en `genai.retrieval_eval` (detalle), `genai.retrieval_eval_summary` y MLflow.

# COMMAND ----------

# MAGIC %pip install -q mlflow
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "andina_dev")
catalog = dbutils.widgets.get("catalog")

import json
import os
from datetime import datetime, timezone

import mlflow
from databricks.sdk import WorkspaceClient
from mlflow.deployments import get_deploy_client

VS_ENDPOINT = f"vs-{catalog.replace('_', '-')}"
INDEX_NAME = f"{catalog}.genai.doc_chunks_index"
# Los endpoints de Claude figuran en el workspace pero con cuota 0; Llama 3.3 70B responde bien en
# español, devuelve texto plano y soporta tool calling (nivel 6).
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
K = 5
COLUMNS = ["chunk_id", "doc_id", "section", "content", "source"]

w = WorkspaceClient()
golden = json.load(open(os.path.join(os.getcwd(), "golden_set.json"), encoding="utf-8"))
run_ts = datetime.now(timezone.utc)


def search(question, query_type, k=K):
    res = w.vector_search_indexes.query_index(
        index_name=INDEX_NAME, columns=COLUMNS, query_text=question, num_results=k, query_type=query_type)
    cols = [c.name for c in res.manifest.columns]
    return [dict(zip(cols, row)) for row in (res.result.data_array or [])]


def is_relevant(hit, relevant):
    return any(hit["doc_id"] == doc and (section is None or hit["section"] == section) for doc, section in relevant)

# COMMAND ----------

# MAGIC %md ## Recall@k y MRR

# COMMAND ----------

details, summary = [], {}
for query_type in ["ANN", "HYBRID"]:
    ranks = []
    for item in golden:
        hits = search(item["q"], query_type)
        rank = next((i + 1 for i, h in enumerate(hits) if is_relevant(h, item["relevant"])), None)
        ranks.append(rank)
        details.append((run_ts, query_type, item["q"], json.dumps(item["relevant"], ensure_ascii=False),
                        rank, [f"{h['doc_id']} > {h['section']}" for h in hits]))
    n = len(ranks)
    summary[query_type] = {
        **{f"recall_at_{k}": sum(1 for r in ranks if r is not None and r <= k) / n for k in (1, 3, 5)},
        "mrr": sum(1 / r for r in ranks if r is not None) / n,
    }

for query_type, m in summary.items():
    print(query_type, {k: round(v, 3) for k, v in m.items()})

spark.createDataFrame(
    details, "run_ts TIMESTAMP, query_type STRING, question STRING, relevant STRING, first_relevant_rank INT, retrieved ARRAY<STRING>"
).write.mode("append").saveAsTable(f"{catalog}.genai.retrieval_eval")
spark.createDataFrame(
    [(run_ts, qt, m["recall_at_1"], m["recall_at_3"], m["recall_at_5"], m["mrr"], len(golden)) for qt, m in summary.items()],
    "run_ts TIMESTAMP, query_type STRING, recall_at_1 DOUBLE, recall_at_3 DOUBLE, recall_at_5 DOUBLE, mrr DOUBLE, questions INT",
).write.mode("append").saveAsTable(f"{catalog}.genai.retrieval_eval_summary")

mlflow.set_experiment(f"/Shared/andina_market/{catalog}_rag_retrieval")
with mlflow.start_run(run_name="golden_set"):
    mlflow.log_params({"embedding": "databricks-gte-large-en", "chunking": "por sección ##",
                       "k": K, "questions": len(golden)})
    for qt, m in summary.items():
        mlflow.log_metrics({f"{qt.lower()}_{k}": v for k, v in m.items()})

# Las preguntas que fallan son las que hay que mirar para mejorar chunking o documentos.
for row in details:
    if row[4] is None or row[4] > 3:
        print(f"[{row[1]}] rango={row[4]} · {row[2]}\n   esperado: {row[3]}\n   recuperado: {row[5][:3]}")

# COMMAND ----------

# MAGIC %md ## Consultas de ejemplo con respuesta citada

# COMMAND ----------

llm = get_deploy_client("databricks")
SYSTEM = (
    "Eres el asistente de atención al cliente de Andina Market. Responde en español, en 2 a 4 frases, "
    "usando SOLO la información de los fragmentos. Si los fragmentos no alcanzan para responder, dilo y "
    "sugiere escribir al chat de atención. Cita al final las fuentes usadas como [n]."
)
BEST = max(summary, key=lambda qt: (summary[qt]["recall_at_3"], summary[qt]["mrr"]))
EXAMPLES = [
    "Me cobraron dos veces mi pedido hace una semana y no me devuelven, ¿qué hago?",
    "¿Puedo devolver un perfume que ya abrí?",
    "¿Cuánto demora el envío a Arequipa y cuánto cuesta si compro 45 dólares?",
    "¿Qué necesito para ser VIP y qué beneficios tiene?",
    "¿La arrocera Mistura de 2.8 L tiene garantía y cuánto cuesta?",
]
examples = []
for q in EXAMPLES:
    hits = search(q, BEST, k=4)
    context = "\n\n".join(f"[{i + 1}] {h['content']}" for i, h in enumerate(hits))
    resp = llm.predict(endpoint=LLM_ENDPOINT, inputs={
        "messages": [{"role": "system", "content": SYSTEM},
                     {"role": "user", "content": f"Fragmentos:\n{context}\n\nPregunta: {q}"}],
        "max_tokens": 400, "temperature": 0,
    })
    answer = resp["choices"][0]["message"]["content"]
    sources = [f"[{i + 1}] {h['doc_id']} > {h['section']}" for i, h in enumerate(hits)]
    examples.append((run_ts, q, BEST, sources, answer))
    print(f"\n### {q}\nRecuperado ({BEST}): " + " | ".join(sources) + f"\nRespuesta: {answer}")

spark.createDataFrame(
    examples, "run_ts TIMESTAMP, question STRING, query_type STRING, sources ARRAY<STRING>, answer STRING"
).write.mode("append").saveAsTable(f"{catalog}.genai.rag_examples")

# Umbral mínimo de calidad: si el retrieval empeora (por ejemplo, tras cambiar documentos o
# chunking), la tarea falla en lugar de dejar un índice malo en uso.
if summary[BEST]["recall_at_5"] < 0.8:
    raise AssertionError(f"recall@5 = {summary[BEST]['recall_at_5']:.2f} < 0.80")
