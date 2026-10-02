# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Herramientas del agente como funciones de Unity Catalog
# MAGIC
# MAGIC Cada herramienta es una función SQL en `<catalog>.genai`: gobernada (permiso `EXECUTE`), con
# MAGIC linaje hacia las tablas que lee y auditada en los logs de Unity Catalog. El comentario de cada
# MAGIC función es la descripción que ve el modelo para decidir cuándo usarla.
# MAGIC
# MAGIC | Herramienta | Tipo | Para qué |
# MAGIC |---|---|---|
# MAGIC | `get_customer_orders` | Datos del cliente (solo lectura) | Últimos pedidos del cliente autenticado |
# MAGIC | `get_order_payments` | Datos del cliente (solo lectura) | Pagos de un pedido **del propio cliente**, con dobles cobros |
# MAGIC | `shipping_quote` | Cálculo determinista | Plazo y costo de envío según la política |
# MAGIC | `refund_eta` | Cálculo determinista | Plazo de reembolso según el método de pago |
# MAGIC | `search_policies` | RAG | Fragmentos de políticas, FAQs, manuales y catálogo (Vector Search) |
# MAGIC
# MAGIC **Ninguna devuelve datos personales** (nombre, email, teléfono) **ni modifica nada**. Las de datos
# MAGIC filtran siempre por `p_customer_id`, que el agente toma de la sesión autenticada, no del modelo.

# COMMAND ----------

dbutils.widgets.text("catalog", "andina_dev")
c = dbutils.widgets.get("catalog")
EFFECTIVE = "('Pagado','Enviado','Entregado')"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {c}.genai.get_customer_orders(
    p_customer_id INT COMMENT 'Id del cliente autenticado (lo fija la aplicación, no el usuario)',
    p_limit INT COMMENT 'Cantidad máxima de pedidos, del más reciente al más antiguo')
RETURNS TABLE (order_id INT, order_date DATE, channel STRING, order_status STRING,
               total_usd DECIMAL(14,2), items INT, has_double_charge BOOLEAN)
COMMENT 'Devuelve los últimos pedidos del cliente autenticado: fecha, canal, estado (Pendiente, Pagado, Enviado, Entregado, Cancelado, Devuelto), total en USD, cantidad de productos y si tuvo un doble cobro. Usar para preguntas sobre "mis pedidos", el estado de una compra o para encontrar el número de pedido.'
RETURN
  -- LIMIT y QUALIFY no aceptan parámetros de la función: se numeran los pedidos del más reciente
  -- al más antiguo en una subconsulta y se filtra afuera.
  SELECT order_id, order_date, channel, order_status, total_usd, items, has_double_charge
  FROM (
    SELECT o.order_id, to_date(o.order_date) AS order_date, o.channel, o.status AS order_status,
           o.total_amount AS total_usd, CAST(coalesce(i.n, 0) AS INT) AS items,
           d.order_id IS NOT NULL AS has_double_charge,
           row_number() OVER (ORDER BY o.order_date DESC) AS rn
    FROM {c}.silver.orders o
    LEFT JOIN (SELECT order_id, count(*) n FROM {c}.silver.order_items GROUP BY order_id) i
      ON i.order_id = o.order_id
    LEFT JOIN (SELECT DISTINCT order_id FROM {c}.silver.payment_double_charges) d
      ON d.order_id = o.order_id
    WHERE o.customer_id = p_customer_id)
  WHERE rn <= p_limit
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {c}.genai.get_order_payments(
    p_customer_id INT COMMENT 'Id del cliente autenticado (lo fija la aplicación, no el usuario)',
    p_order_id INT COMMENT 'Número de pedido')
RETURNS TABLE (order_id INT, payment_id INT, method STRING, payment_status STRING, amount_usd DECIMAL(14,2),
               payment_date TIMESTAMP, is_double_charge BOOLEAN, double_charge_refunded BOOLEAN, next_step STRING)
COMMENT 'Devuelve los pagos de un pedido DEL CLIENTE AUTENTICADO: método, estado (Pendiente, Aprobado, Rechazado, Reembolsado), monto, fecha, si es un cobro duplicado, si ya se devolvió y el siguiente paso según la política (next_step). Si el pedido no es del cliente, no devuelve filas. Usar para preguntas de cobros, rechazos, reembolsos o dobles cobros de un pedido. Comunicar next_step tal cual.'
RETURN
  SELECT p.order_id, p.payment_id, p.method, p.status, p.amount, p.payment_date,
         d.duplicate_payment_id IS NOT NULL,
         coalesce(d.duplicate_current_status = 'Reembolsado', false),
         -- Regla de la política de pagos, aplicada de forma determinista (no la deduce el modelo).
         CASE
           WHEN d.duplicate_payment_id IS NULL THEN NULL
           WHEN d.duplicate_current_status = 'Reembolsado' THEN 'Este cobro duplicado ya fue devuelto.'
           WHEN p.payment_date < current_timestamp() - INTERVAL 72 HOURS THEN
             'Cobro duplicado sin devolver hace más de 72 horas: escalar a un agente humano por chat con prioridad URGENTE.'
           ELSE 'Cobro duplicado detectado: se devuelve automáticamente dentro de las 72 horas desde el pago.'
         END
  FROM {c}.silver.payments p
  JOIN {c}.silver.orders o ON o.order_id = p.order_id AND o.customer_id = p_customer_id
  LEFT JOIN {c}.silver.payment_double_charges d ON d.duplicate_payment_id = p.payment_id
  WHERE p.order_id = p_order_id
  ORDER BY p.payment_date
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {c}.genai.shipping_quote(
    p_city STRING COMMENT 'Ciudad de destino, por ejemplo Arequipa',
    p_order_amount DOUBLE COMMENT 'Monto del pedido en USD')
RETURNS TABLE (city STRING, zone STRING, delivery_time STRING, shipping_cost_usd DOUBLE, rule STRING)
COMMENT 'Calcula el plazo y el costo de envío según la política de envíos: capitales (Lima, Bogotá, Santiago, Ciudad de México, Quito) 1 a 3 días hábiles; otras ciudades principales 2 a 5; resto del país 4 a 8. Envío gratis desde 60 USD; si no, 4,90 USD en capitales y 7,90 USD en el resto. Usar SIEMPRE para cualquier pregunta de costo o plazo de envío: no calcularlo a partir de texto.'
RETURN
  WITH n AS (
    SELECT lower(translate(trim(p_city), 'ÁÉÍÓÚáéíóú', 'AEIOUaeiou')) AS k),
  z AS (
    SELECT CASE
      WHEN k IN ('lima', 'bogota', 'santiago', 'ciudad de mexico', 'cdmx', 'quito') THEN 'capital'
      WHEN k IN ('arequipa', 'trujillo', 'cusco', 'piura', 'medellin', 'cali', 'barranquilla', 'valparaiso',
                 'concepcion', 'guadalajara', 'monterrey', 'puebla', 'guayaquil', 'cuenca') THEN 'ciudad principal'
      ELSE 'resto del pais' END AS zone FROM n)
  SELECT p_city, zone,
         CASE zone WHEN 'capital' THEN '1 a 3 días hábiles' WHEN 'ciudad principal' THEN '2 a 5 días hábiles'
                   ELSE '4 a 8 días hábiles' END,
         CASE WHEN p_order_amount >= 60 THEN 0.0 WHEN zone = 'capital' THEN 4.90 ELSE 7.90 END,
         'Política de envíos: gratis desde 60 USD; 4,90 USD en capitales y 7,90 USD en el resto del país; plazos desde la aprobación del pago'
  FROM z
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {c}.genai.refund_eta(
    p_method STRING COMMENT 'Método de pago: tarjeta, billetera, transferencia o efectivo')
RETURNS TABLE (method STRING, refund_time STRING, rule STRING)
COMMENT 'Devuelve el plazo de reembolso según el método de pago, contado desde que Andina Market recibe y revisa el producto (hasta 2 días hábiles). Usar para preguntas de cuándo llega un reembolso.'
RETURN
  SELECT p_method,
         CASE lower(trim(p_method))
           WHEN 'tarjeta' THEN '5 a 10 días hábiles, según el banco'
           WHEN 'billetera' THEN 'hasta 48 horas'
           WHEN 'transferencia' THEN '3 a 5 días hábiles'
           WHEN 'efectivo' THEN 'vale de compra inmediato o transferencia en 3 a 5 días hábiles'
           ELSE 'método no reconocido: consultar con atención al cliente' END,
         'Política de devoluciones: el reembolso se hace al mismo método de pago'
""")

# RAG: búsqueda híbrida en el índice del nivel 5 (requiere el endpoint de Vector Search activo).
spark.sql(f"""
CREATE OR REPLACE FUNCTION {c}.genai.search_policies(
    p_question STRING COMMENT 'Pregunta o tema a buscar, en español')
RETURNS TABLE (doc_id STRING, section STRING, content STRING)
COMMENT 'Busca en las políticas, preguntas frecuentes, manuales y catálogo de Andina Market y devuelve los 4 fragmentos más relevantes con su documento y sección. Usar para preguntas de devoluciones, garantías, pagos, cuenta, programa de clientes, atención y productos. Citar doc_id y section en la respuesta.'
RETURN
  SELECT doc_id, section, content
  FROM vector_search(index => '{c}.genai.doc_chunks_index', query_text => p_question,
                     num_results => 4, query_type => 'HYBRID')
""")
print("Herramientas creadas en", f"{c}.genai")
