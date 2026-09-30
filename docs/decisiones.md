# Registro de decisiones de arquitectura

Cada decisión incluye la alternativa descartada y el motivo. Este registro crece nivel por nivel.

## D-01. Ambiente: Azure free trial + Azure Databricks Premium

- **Decisión:** usar una suscripción free trial de Azure (con posibilidad de pasarla a Pago por uso) y un workspace de Azure Databricks en tier Premium (`ws_richard`), en East US.
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

## D-06. Región de Azure SQL: Central US en lugar de la región del workspace

- **Decisión:** el servidor `sql-andina-cus` (base `andina_oltp`, oferta gratuita serverless) está en **Central US**, en el grupo de recursos `rg-andina-market`.
- **Descartado: East US, la misma región del workspace (y East US 2).** Era la opción preferida porque evita el tráfico entre regiones, pero Azure tiene restringida la creación de servidores SQL nuevos en East US y East US 2 para esta suscripción (`RegionDoesNotAllowProvisioning`). Se puede pedir una excepción por soporte, pero no está garantizada y retrasa el nivel 0.
- **Impacto:** la extracción JDBC desde Databricks cruza regiones. Con este volumen, el costo de salida de datos y la latencia adicional son despreciables. Se validó que el cómputo serverless lee la base por JDBC. En producción, la fuente y el lakehouse estarían en la misma región, conectados por Private Endpoint.

## D-07. Almacenamiento y organización en Unity Catalog

- **Decisión:** una cuenta ADLS Gen2 propia (`standinamarket706`, contenedor `lakehouse`, East US) registrada en Unity Catalog mediante el Access Connector `ac-andina-market` (identidad administrada con *Storage Blob Data Contributor*), la credencial `cred_andina_lakehouse` y la ubicación externa `loc_andina_lakehouse`.
- **Un catálogo por entorno:** `andina_dev` y `andina_prod`, cada uno con su propia raíz de almacenamiento. El código recibe el catálogo como parámetro y el bundle lo fija por target, así el mismo código se promueve sin cambios. Staging se omite por el tamaño del reto: se agregaría como un tercer catálogo y un tercer target.
- **Esquemas por capa:** `landing`, `bronze`, `silver`, `gold`, `ml`, `genai`, más `ops` para el control de la ingesta (última versión de Change Tracking leída por tabla y registro de lotes). Separar `ops` evita mezclar metadatos operativos con datos de negocio y permite darle permisos distintos.
- **Descartado: el catálogo por defecto del workspace (`ws_richard`).** Su almacenamiento vive en el grupo de recursos administrado por Databricks, así que se borra con el workspace y no se puede gobernar de forma independiente.
- **Credenciales de la fuente:** en el secret scope `andina-sql` (`server`, `database`, `user`, `password`), nunca en el código.

## D-08. Ingesta en dos pasos: landing en Parquet y bronze con Auto Loader

- **Decisión:** la extracción JDBC escribe cada lote como Parquet en `/Volumes/<catalog>/landing/sqlserver/<tabla>/<batch_id>/`, y Auto Loader lo carga a `<catalog>.bronze.<tabla>`.
- **Descartado: JDBC directo a bronze.** Acopla la lectura de la fuente con la escritura en el lakehouse. Con landing, si falla la carga a bronze se reprocesan los archivos sin volver a consultar el OLTP, y queda una copia auditable de lo extraído en cada lote.
- **Descartado: Lakeflow Connect (conector gestionado de SQL Server).** Resuelve el mismo problema con menos código, pero requiere un gateway de ingesta sobre cómputo clásico, que compite con la cuota de vCPU de la suscripción (D-01), y oculta los mecanismos que el reto pide explicar (checkpoints, marca de agua, evolución de esquema). En un proyecto real con más tablas sería la primera opción a evaluar.
- **Descartado: Lakehouse Federation.** Permite consultar Azure SQL desde Unity Catalog sin copiar datos, pero no guarda historial ni captura cambios: sirve para exploración, no para ingesta.
- **Por qué Parquet en landing y no Delta:** landing es una zona de intercambio de archivos inmutables; Parquet es el formato natural para Auto Loader y el mismo patrón sirve para otras fuentes que dejan archivos (SAP vía ADF, exportaciones).

## D-09. Marca de agua de Change Tracking y garantías de entrega

- **Una versión de corte por lote:** al inicio se lee `CHANGE_TRACKING_CURRENT_VERSION()` una sola vez y todas las tablas se extraen hasta esa versión (`SYS_CHANGE_VERSION <= to_version`). Así el lote representa un mismo punto de la fuente para las 6 tablas.
- **La versión se guarda después de escribir:** `ops.ct_watermarks` se actualiza solo cuando el Parquet del lote ya está escrito. Si algo falla antes, la próxima corrida repite desde la versión anterior.
- **Garantía: at-least-once en la extracción, exactly-once entre landing y bronze.** Un reintento puede volver a extraer cambios ya extraídos, y una fila que cambia durante el snapshot inicial llega otra vez en el incremental con una versión mayor. Por eso bronze es un registro de cambios (no un espejo) y silver se queda con la última versión por PK (`_ct_version`). Entre landing y bronze, el checkpoint de Auto Loader garantiza que cada archivo se procesa una sola vez; se comprobó reanudando una tarea fallida sin duplicar filas.
- **Descartado: snapshot isolation en una transacción con la lectura de la versión.** Es la forma exacta que documenta Microsoft, pero Spark JDBC abre una conexión por consulta y no comparte transacción entre la versión y el SELECT. La idempotencia aguas abajo cubre el mismo riesgo con menos complejidad.
- **Recarga automática:** si la versión guardada es menor que `CHANGE_TRACKING_MIN_VALID_VERSION` (el extractor no corrió dentro de la retención de 7 días), esa tabla se recarga completa y el motivo queda en `ingestion_log.mode_reason`.
- **Reintentos idempotentes:** el `batch_id` es el `run_id` del job y el lote se escribe con `overwrite`: si Databricks reintenta la tarea, reemplaza el mismo lote en vez de crear uno nuevo. `max_concurrent_runs: 1` evita que dos corridas compitan por la misma marca de agua.
- **Fallas parciales:** si una tabla falla, se registra en `ingestion_log` con el error, las demás siguen y la tarea termina en error para que el job la reintente y avise por correo.

## D-10. Evolución de esquema y trazabilidad

- **Detección en la extracción:** en cada corrida se leen las columnas de `INFORMATION_SCHEMA` y se comparan con las de la extracción anterior (guardadas en `ct_watermarks`). Columnas nuevas o quitadas quedan en `ingestion_log` (`schema_changed`, `added_columns`, `removed_columns`). La extracción siempre usa la lista de columnas vigente, así que una columna nueva viaja sola.
- **Adopción en bronze:** Auto Loader con `schemaEvolutionMode = addNewColumns` y `mergeSchema`. Ante una columna nueva, el stream se detiene, guarda el esquema ampliado y el notebook lo reinicia. Las filas anteriores quedan con la columna en NULL. Datos que no encajan en el esquema van a `_rescued_data` en lugar de perderse.
- **Descartado: fallar ante cualquier cambio de esquema.** Es más estricto, pero detiene toda la ingesta por una columna nueva que no rompe nada. Una columna quitada o un cambio de tipo sí deben revisarse: quedan registrados en el log y el contrato se valida en silver.
- **Trazabilidad por fila:** `_source` (base, esquema y tabla de origen), `_batch_id` (corrida del job), `_ct_version` y `_ct_operation` (`S` snapshot, `I`, `U`, `D`), `_extracted_at`, `_ingested_at` y `_source_file` (archivo exacto de landing). Con eso se responde cuándo y de dónde llegó cada dato, y se une con `ingestion_log` por `batch_id`.
- **Bronze append-only:** `delta.appendOnly = true` impide UPDATE y DELETE sobre bronze. Los DELETE de la fuente llegan como filas con `_ct_operation = 'D'` y solo la PK.

## D-11. Orquestación y frecuencia

- **Lakeflow Job de dos tareas** (extracción → bronze) en cómputo serverless, definido en un Declarative Automation Bundle con targets `dev` y `prod`. Cada target fija su catálogo, así que el mismo código se promueve sin cambios.
- **Frecuencia: cada hora.** Change Tracking entrega el estado neto por PK, así que una frecuencia mayor reduce los estados intermedios de `Payments` que no se llegan a ver (D-03). La retención de 7 días da margen amplio ante fallas.
- **Schedule en pausa por defecto** en ambos targets: cada corrida consume DBU, así que se activa a propósito al pasar a producción.
- **Streaming con costo de batch:** bronze usa `trigger(availableNow=True)`. Pasar a tiempo casi real solo requiere cambiar el trigger a `processingTime` y ejecutar el stream de forma continua; la lógica, los checkpoints y las tablas no cambian.
