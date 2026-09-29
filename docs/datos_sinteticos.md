# Base de origen y datos sintéticos

## Cómo se construyó el dataset

El generador (`data_generator/`) no produce filas al azar. Simula el comportamiento del negocio y deja que las tablas salgan de ese comportamiento:

1. **Catálogo:** 320 productos en 6 categorías con marcas ficticias. Cada tipo de producto tiene su rango de precio y una descripción con atributos concretos (garantía, material, capacidad), que después alimenta el nivel RAG. El 20 % de los productos se lanza durante el período y el 6 % se descontinúa.
2. **Clientes con comportamiento oculto:** 5 países con pesos (Perú 35 %) y ciudades reales. Cada cliente tiene:
   - una tasa de compra con distribución gamma (pocos compran mucho),
   - una fecha de abandono (churn),
   - una categoría favorita,
   - una preferencia de canal.

   El 12 % se registra y nunca compra.
3. **Pedidos:** proceso de Poisson por cliente, con estacionalidad mensual (noviembre ×1,6, diciembre ×1,5, julio ×1,2), por día de semana y por hora (picos al almuerzo y en la noche). El canal app gana participación con el tiempo (del 25 % al 42 %).
4. **Precios históricos:** `OrderItems.UnitPrice` guarda el precio del momento, con inflación del 4 % anual y descuentos en campañas. `Products.Price` es solo el precio vigente. Así, el historial de precios existe en la fuente de forma implícita.
5. **Pagos con ciclo de vida:**
   - rechazos por método (tarjeta 9 %), con reintentos y cambio de medio de pago;
   - las transferencias nacen *Pendiente* y se aprueban horas después;
   - las devoluciones pasan el pago a *Reembolsado*.
6. **Tickets:** nacen de eventos reales, como un pago rechazado (`pago_rechazado`), una devolución (`reembolso`) o un doble cobro (`cobro_duplicado`).
   - La prioridad la asigna un "agente": depende del tipo de ticket y sube cuando el texto expresa urgencia.
   - Tiene un 8 % de ruido de etiquetado. Es un problema de clasificación realista y no trivial.
7. **Segmento:** lo calcula una regla de CRM sobre los últimos 365 días (`config.SEGMENT_RULES`). El simulador de cambios la vuelve a aplicar, y cada cambio de segmento genera una versión SCD2 aguas abajo.

## Volumetría (semilla 42, histórico del 1 de octubre de 2024 al 28 de septiembre de 2026)

| Tabla | Filas |
|---|---:|
| Customers | 6.057 |
| Products | 320 |
| Orders | 28.101 |
| OrderItems | 50.145 |
| Payments | 29.581 |
| SupportTickets | 2.721 |

El ticket promedio (AOV) es de 123 USD y la mediana, de 68 USD. Mezcla de canales: web 42 %, app 34 %, tienda 24 %.

## Casos borde inyectados a propósito

| Caso | Cantidad | Qué representa | Tratamiento previsto en el lakehouse |
|---|---:|---|---|
| Cliente duplicado (segunda cuenta en tienda, email con otra forma) | 57 | Registro doble en un sistema legacy | Silver normaliza el email (trim + lower). Se marca el grupo de duplicados sin fusionar a ciegas |
| Email nulo | 445 | Clientes de tienda física | Válido: se acepta el nulo |
| Email con formato inválido | 30 | Error de digitación | Expectation `warn` y bandera `email_valido` |
| Teléfono o ciudad nulos | 664 / 183 | Datos incompletos | Se aceptan; la ciudad nula se reporta como "Desconocida" |
| País con formato sucio (`Peru`, ` pe`, `MEX`) | 121 | Captura libre | Mapeo a ISO-2 en silver |
| Total del pedido ≠ suma de líneas (cupón no itemizado) | 140 | Descuento aplicado solo a la cabecera | Expectation `warn` y columna `diferencia_total` |
| Pedido sin líneas | 56 | Bug de la app | Se detecta con una regla de integridad y se reporta |
| Línea con cantidad 0 | 50 | Error de captura | Expectation `drop` |
| Línea con `ProductId` inexistente (FK `WITH NOCHECK`) | 25 | Migración legacy | Tabla de cuarentena, no se descarta en silencio |
| Fecha de pedido en 2027 (año mal digitado) | 3 | Error de captura | Expectation `warn`; se usa `CreatedAt` como fecha confiable |
| Pago aprobado duplicado (doble clic, pocos segundos de diferencia) | 70 | Doble cobro real, que suele generar un ticket urgente | Regla de detección y KPI de doble cobro |
| Ticket con cuerpo vacío (`''`, no `NULL`) | ~51 | El cliente solo llenó el asunto | Se normaliza `''` a `NULL` en silver |
| Tickets spam | 6 | Entran por el formulario web | El simulador los elimina con `DELETE`, y la ingesta debe detectar esa eliminación |
| Productos descontinuados con ventas históricas | 20 | Ciclo de vida del catálogo | Dimensión con estado; las ventas pasadas se conservan |
| Clientes sin pedidos | ~1.600 | Registro sin conversión | Válido; es útil para features y KPIs de conversión |

## Cambios incrementales (`simulate_changes.py`)

Cada ejecución simula un día de operación dentro de una sola transacción:

- **INSERT:** clientes nuevos, pedidos, líneas, pagos (incluidos reintentos), tickets.
- **UPDATE:**
  - pagos *Pendiente* → *Aprobado* / *Rechazado*;
  - pedidos *Pagado* → *Enviado* → *Entregado*;
  - devoluciones con reembolso;
  - segmentos del CRM, mudanzas de clientes, precios, productos descontinuados;
  - estado de los tickets.
- **DELETE:** tickets spam y una línea quitada de un pedido pendiente.
- **`--schema-change`:** agrega `Orders.CouponCode` para probar la evolución de esquema de punta a punta.

## Muestra de clickstream (`sample_data/clickstream_app_events.jsonl`)

Son 6.559 eventos de 900 sesiones del app en dos días: `session_start`, `product_view`, `add_to_cart`, `remove_from_cart`, `checkout_start`, `purchase`. Incluye los problemas de un stream real:

- Unos 2 % de duplicados por reenvío del SDK (at-least-once), con el mismo `event_id`.
- Un 3 % de eventos tardíos: el `sent_ts` llega horas después del `event_ts`.
- Unos 1.600 eventos anónimos, con `customer_id` nulo.
- Deriva de esquema: la versión 5.x del app agrega el campo `campaign`.
