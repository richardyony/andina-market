# Registro de decisiones de arquitectura

Cada decisión incluye la alternativa descartada y el motivo. Este registro crece nivel por nivel.

## D-01. Ambiente: Azure free trial + Azure Databricks Premium

- **Decisión:** usar una suscripción free trial de Azure (con posibilidad de pasarla a Pago por uso) y un workspace de Azure Databricks en tier Premium, en East US 2.
- **Descartado: Databricks Free Edition.** Tiene restricciones de red saliente y de cuotas que ponen en riesgo la conexión JDBC con Azure SQL, que es el camino crítico del nivel 1.
- **Descartado: Azure for Students.** No permite aumentar la cuota de vCPU y restringe las regiones y los tipos de VM.
- **Mitigación de cuota:** las cuentas de prueba tienen de 4 a 6 vCPU regionales, así que el cómputo es **serverless** (notebooks, Jobs, Lakeflow Declarative Pipelines y SQL warehouse). Serverless corre en infraestructura gestionada por Databricks y no consume la cuota de la suscripción. El plan B es un cluster single node de 4 cores.
- **Por qué Premium:** Unity Catalog, serverless y el control de acceso granular lo requieren.

## D-02. Red: conectividad segura de cluster sí, VNet injection no

- **Decisión:** activar Secure Cluster Connectivity (sin IP pública en los nodos) y usar la VNet administrada.
- **Descartado para el reto: VNet injection.** Obliga a crear subnets delegadas y NSGs, y no aporta nada si se trabaja en serverless.
- **En producción:** VNet injection, Private Endpoints hacia Azure SQL y ADLS, y VPN o ExpressRoute hacia la red on-premise. Esto último es un requisito para integrar SAP ECC.

## D-03. Detección de cambios en la fuente: Change Tracking

- **Decisión:** usar SQL Server Change Tracking (CT) en las 6 tablas, con retención de 7 días.
- **Descartado: watermark por `UpdatedAt`.** No detecta DELETEs y depende de que la aplicación siempre actualice la columna.
- **Descartado: CDC.** Guarda cada transición intermedia en tablas de historial dentro de la fuente, lo que implica más costo, más almacenamiento y más carga para el OLTP.
- **Trade-off asumido:** CT devuelve el estado neto por PK. Si un pago cambia dos veces entre dos extracciones, solo se ve el último estado. El historial de estados se construye en bronze, que es append-only. Si negocio necesitara cada transición intermedia de `Payments`, activaríamos CDC solo en esa tabla.
- **Recuperación:** si el extractor no corre durante la retención, la versión guardada queda por debajo de `CHANGE_TRACKING_MIN_VALID_VERSION` y el extractor hace una recarga completa de esa tabla.
- **Orden de activación:** CT se activa después de la carga histórica. El histórico representa lo que "ya existía", y la primera extracción es un snapshot consistente (snapshot isolation) junto con su versión de CT.

## D-04. Fuente permisiva, calidad aguas abajo

- **Decisión:** la fuente tiene pocos CHECK constraints, no tiene UNIQUE en el email, y la FK `OrderItems → Products` es `WITH NOCHECK`.
- **Motivo:** simula un OLTP legacy real. La calidad se garantiza en el lakehouse, con expectations y cuarentena, y no se asume en la fuente.

## D-05. Supuestos del dataset

- **Moneda:** todos los montos están en USD. En un escenario real, cada país factura en moneda local y habría una tabla de tipos de cambio.
- **Zona horaria:** todas las fechas están en UTC.
- **Totales:** `TotalAmount` no incluye el envío; es la suma de las líneas, salvo en los casos borde documentados.
- **Segmento:** existe solo como estado actual en la fuente. El historial SCD2 empieza con la primera ingesta.
- **Contenido ficticio:** marcas y personas son ficticias; los nombres se generan con Faker.
