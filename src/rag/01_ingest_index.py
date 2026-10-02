# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Documentos → chunks → índice vectorial
# MAGIC
# MAGIC 1. **Documentos en un Volume de Unity Catalog** (`genai.docs`): las políticas, FAQs y manuales
# MAGIC    versionados en `rag_docs/` se publican en el Volume, que es la copia gobernada (permisos y linaje).
# MAGIC 2. **Chunking por sección:** cada sección `##` de un documento es un chunk, con el título del
# MAGIC    documento y de la sección al inicio. Una sección responde una pregunta concreta ("plazos de
# MAGIC    reembolso"); cortar por cantidad fija de caracteres partiría tablas y listas a la mitad.
# MAGIC    Las secciones largas se dividen por párrafos. El catálogo aporta un chunk por producto.
# MAGIC 3. **Tabla `genai.doc_chunks`** con Change Data Feed, actualizada con `MERGE`: solo cambia lo que
# MAGIC    cambió (por hash del contenido) y se borran los chunks de secciones o documentos eliminados.
# MAGIC 4. **Índice Delta Sync de Vector Search** sobre esa tabla, con embeddings gestionados por
# MAGIC    `databricks-gte-large-en`. Al sincronizar, el índice toma solo los cambios.

# COMMAND ----------

dbutils.widgets.text("catalog", "andina_dev")
catalog = dbutils.widgets.get("catalog")

import hashlib
import os
import re
import time
from datetime import timedelta

# Se usa el SDK de Databricks (ya incluido en serverless) y no el paquete databricks-vectorsearch:
# instalarlo baja la versión de protobuf y rompe el entorno de Spark Connect.
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest, EmbeddingSourceColumn, EndpointStatusState, EndpointType,
    PipelineType, VectorIndexType,
)
from pyspark.sql import functions as F

# databricks-qwen3-embedding-0-6b (multilingüe) aparece en la lista de endpoints pero no está
# desplegado en este workspace: se usa gte-large-en y el golden set mide su efecto en español.
EMBEDDING_ENDPOINT = "databricks-gte-large-en"
VS_ENDPOINT = f"vs-{catalog.replace('_', '-')}"
CHUNKS_TABLE = f"{catalog}.genai.doc_chunks"
INDEX_NAME = f"{catalog}.genai.doc_chunks_index"
VOLUME_DIR = f"/Volumes/{catalog}/genai/docs"
REPO_DOCS = os.path.abspath(os.path.join(os.getcwd(), "..", "..", "rag_docs"))
MAX_CHARS = 1200  # una sección más larga se divide por párrafos

# COMMAND ----------

# MAGIC %md ## 1. Publicar los documentos en el Volume

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.genai.docs "
          "COMMENT 'Documentos fuente del RAG: políticas, FAQs y manuales de Andina Market'")
repo_files = {f for f in os.listdir(REPO_DOCS) if f.endswith(".md")}
for f in repo_files:
    with open(os.path.join(REPO_DOCS, f), encoding="utf-8") as src, \
         open(os.path.join(VOLUME_DIR, f), "w", encoding="utf-8") as dst:
        dst.write(src.read())
# Un documento borrado del repositorio se borra del Volume (y después, sus chunks del índice).
for f in os.listdir(VOLUME_DIR):
    if f.endswith(".md") and f not in repo_files:
        os.remove(os.path.join(VOLUME_DIR, f))
print(f"{len(repo_files)} documentos en {VOLUME_DIR}")

# COMMAND ----------

# MAGIC %md ## 2. Chunking por sección

# COMMAND ----------

def doc_type(doc_id):
    for prefix, kind in [("politica_", "politica"), ("faq_", "faq"), ("manual_", "manual"), ("guia_", "manual")]:
        if doc_id.startswith(prefix):
            return kind
    return "informacion"


def linearize_tables(text):
    """Convierte cada fila de una tabla Markdown en una frase "Columna: valor; Columna: valor".

    Los modelos de embeddings representan mal las filas con barras '|': la primera evaluación mostró
    que las preguntas cuya respuesta estaba en una tabla (plazos de reembolso, métodos de pago por
    canal) no se recuperaban. Así cada fila queda como texto con su contexto de columna.
    """
    out, header = [], None
    for line in text.split("\n"):
        if line.strip().startswith("|"):
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if all(re.fullmatch(r":?-{2,}:?", c) for c in cells):
                continue  # separador |---|---|
            if header is None:
                header = cells
                continue
            out.append("; ".join(f"{h}: {v}" for h, v in zip(header, cells) if v) + ".")
        else:
            header = None
            out.append(line)
    return "\n".join(out)


def split_long(text, limit=MAX_CHARS):
    """Divide por párrafos sin cortar uno a la mitad; cada parte arrastra el párrafo previo (solapamiento)."""
    paragraphs = [p for p in text.split("\n\n") if p.strip()]
    parts, current = [], []
    for p in paragraphs:
        if current and len("\n\n".join(current + [p])) > limit:
            parts.append("\n\n".join(current))
            current = [current[-1]]  # solapamiento de un párrafo para no perder contexto
        current.append(p)
    if current:
        parts.append("\n\n".join(current))
    return parts


def chunk_markdown(doc_id, text, source):
    title = re.search(r"^# (.+)$", text, re.M).group(1).strip()
    sections = re.split(r"^## ", text, flags=re.M)
    intro = sections[0].split("\n", 1)[1].strip() if "\n" in sections[0] else ""
    blocks = ([("Introducción", intro)] if intro else []) + [
        (s.split("\n", 1)[0].strip(), s.split("\n", 1)[1].strip() if "\n" in s else "") for s in sections[1:]
    ]
    rows = []
    for section, body in blocks:
        for i, part in enumerate(split_long(linearize_tables(body))):
            content = f"{title} > {section}\n\n{part}"
            rows.append({
                "chunk_id": hashlib.sha1(f"{doc_id}|{section}|{i}".encode()).hexdigest(),
                "doc_id": doc_id, "doc_type": doc_type(doc_id), "title": title, "section": section,
                "part": i, "content": content, "source": source,
            })
    return rows


rows = []
for f in sorted(os.listdir(VOLUME_DIR)):
    if f.endswith(".md"):
        with open(os.path.join(VOLUME_DIR, f), encoding="utf-8") as fh:
            rows += chunk_markdown(f[:-3], fh.read(), f"{VOLUME_DIR}/{f}")

docs_df = spark.createDataFrame(rows)

# Catálogo: un chunk por producto, con sus atributos en texto (el estado indica si se puede comprar).
products_df = spark.table(f"{catalog}.silver.products").select(
    F.sha1(F.concat(F.lit("producto|"), "sku")).alias("chunk_id"),
    F.concat(F.lit("producto:"), "sku").alias("doc_id"),
    F.lit("producto").alias("doc_type"),
    F.lit("Catálogo de productos").alias("title"),
    F.col("product_name").alias("section"),
    F.lit(0).cast("long").alias("part"),
    F.concat_ws("\n",
        F.concat(F.lit("Producto: "), "product_name", F.lit(" (SKU "), "sku", F.lit(")")),
        F.concat(F.lit("Categoría: "), "category", F.lit(" / "), F.coalesce("subcategory", F.lit("-"))),
        F.concat(F.lit("Marca: "), F.coalesce("brand", F.lit("-"))),
        F.concat(F.lit("Precio: "), F.col("price").cast("string"), F.lit(" USD")),
        F.concat(F.lit("Estado: "), F.when(F.col("is_active"), "Disponible").otherwise("Descontinuado")),
        F.coalesce("description", F.lit("")),
    ).alias("content"),
    F.lit(f"{catalog}.silver.products").alias("source"),
)

chunks = (
    docs_df.select(*products_df.columns).unionByName(products_df)
    .withColumn("content_hash", F.sha1("content"))
    .withColumn("updated_at", F.current_timestamp())
)
print(f"{docs_df.count()} chunks de documentos + {products_df.count()} de productos")

# COMMAND ----------

# MAGIC %md ## 3. Tabla de chunks (idempotente, con Change Data Feed)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CHUNKS_TABLE} (
    chunk_id STRING NOT NULL, doc_id STRING, doc_type STRING, title STRING, section STRING,
    part BIGINT, content STRING, source STRING, content_hash STRING, updated_at TIMESTAMP,
    CONSTRAINT doc_chunks_pk PRIMARY KEY (chunk_id)
) TBLPROPERTIES (delta.enableChangeDataFeed = true)
COMMENT 'Chunks del RAG: una sección de documento o un producto por fila. Fuente del índice vectorial.'
""")
chunks.createOrReplaceTempView("new_chunks")
spark.sql(f"""
MERGE INTO {CHUNKS_TABLE} t USING new_chunks s ON t.chunk_id = s.chunk_id
WHEN MATCHED AND t.content_hash <> s.content_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE
""")
n_chunks = spark.table(CHUNKS_TABLE).count()
print(f"{CHUNKS_TABLE}: {n_chunks} chunks")

# COMMAND ----------

# MAGIC %md ## 4. Endpoint e índice de Vector Search

# COMMAND ----------

w = WorkspaceClient()

if VS_ENDPOINT not in [e.name for e in w.vector_search_endpoints.list_endpoints()]:
    w.vector_search_endpoints.create_endpoint_and_wait(
        name=VS_ENDPOINT, endpoint_type=EndpointType.STANDARD, timeout=timedelta(minutes=40))
for _ in range(160):
    state = w.vector_search_endpoints.get_endpoint(VS_ENDPOINT).endpoint_status.state
    if state == EndpointStatusState.ONLINE:
        break
    time.sleep(15)
print(f"Endpoint {VS_ENDPOINT}: {state}")

try:
    w.vector_search_indexes.get_index(INDEX_NAME)
    created = False
except NotFound:
    w.vector_search_indexes.create_index(
        name=INDEX_NAME,
        endpoint_name=VS_ENDPOINT,
        primary_key="chunk_id",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=CHUNKS_TABLE,
            pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[EmbeddingSourceColumn(
                name="content", embedding_model_endpoint_name=EMBEDDING_ENDPOINT)],
            columns_to_sync=["chunk_id", "doc_id", "doc_type", "title", "section", "content", "source"],
        ),
    )
    created = True  # la creación ya dispara la primera sincronización

# Espera a que el índice esté listo; si ya existía, sincroniza solo los cambios (TRIGGERED).
synced = created
for _ in range(240):
    status = w.vector_search_indexes.get_index(INDEX_NAME).status
    if status and status.ready and not synced:
        w.vector_search_indexes.sync_index(INDEX_NAME)
        synced = True
        time.sleep(30)
        continue
    indexed = (status.indexed_row_count or 0) if status else 0
    if status and status.ready and indexed == n_chunks:
        break
    time.sleep(15)
print(f"Índice {INDEX_NAME}: {indexed} de {n_chunks} chunks indexados, listo={status.ready if status else None}")
message = (status.message or "") if status else ""
# El conteo de filas no basta: un índice puede seguir "online" con datos viejos si su última
# sincronización falló (por ejemplo, si el modelo de embeddings no está disponible).
if indexed != n_chunks or "failed" in message.lower():
    raise RuntimeError(f"El índice no quedó sincronizado con la tabla de chunks: {message}")
