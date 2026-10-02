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
- **Credenciales de la fuente:** en Azure Key Vault, leídas por el secret scope `andina-kv` (`server`, `database`, `user`, `password`), nunca en el código (D-17).

## D-08. Ingesta en dos pasos: landing en Parquet y bronze con Auto Loader

- **Decisión:** la extracción JDBC escribe cada lote como Parquet en `/Volumes/<catalog>/landing/sqlserver/<tabla>/<batch_id>/`, y Auto Loader lo carga a `<catalog>.bronze.<tabla>`.
- **Descartado: JDBC directo a bronze.** Acopla la lectura de la fuente con la escritura en el lakehouse. Con landing, si falla la carga a bronze se reprocesan los archivos sin volver a consultar el OLTP, y queda una copia auditable de lo extraído en cada lote.
- **Descartado: Lakeflow Connect (conector gestionado de SQL Server).** Resuelve el mismo problema con menos código, pero requiere un gateway de ingesta sobre cómputo clásico, que compite con la cuota de vCPU de la suscripción (D-01), y oculta los mecanismos que el reto pide explicar (checkpoints, marca de agua, evolución de esquema). En un proyecto real con más tablas sería la primera opción a evaluar.
- **Descartado: Lakehouse Federation.** Permite consultar Azure SQL desde Unity Catalog sin copiar datos, pero no guarda historial ni captura cambios: sirve para exploración, no para ingesta.
- **Por qué Parquet en landing y no Delta:** landing es una zona de intercambio de archivos inmutables; Parquet es el formato natural para Auto Loader y el mismo patrón sirve para otras fuentes que dejan archivos (SAP vía ADF, exportaciones).

## D-09. Marca de agua de Change Tracking y garantías de entrega

- **Una versión de corte por lote:** al inicio se lee `CHANGE_TRACKING_CURRENT_VERSION()` una sola vez y todas las tablas se extraen hasta esa versión (`SYS_CHANGE_VERSION <= to_version`). Así el lote representa un mismo punto de la fuente para las 6 tablas.
- **Garantía: cada rango de versiones se publica una sola vez en landing, y cada archivo se carga una sola vez en bronze.** Se logra con tres piezas:
  1. **Publicación atómica:** el lote se escribe en `landing/sqlserver/_staging/` y, completo, se mueve a `landing/sqlserver/<tabla>/`. Auto Loader solo lee la segunda carpeta, así que nunca ve un lote a medias. Lo que quede en `_staging` de un intento fallido se borra al empezar la tabla.
  2. **Nombre del lote por versión, no por corrida:** `to_v<versión>__<modo>__run_<batch_id>`. Un reintento nunca sobrescribe un lote anterior.
  3. **Marca de agua recuperable:** `ops.ct_watermarks` se actualiza después de publicar. Si el proceso cae entre publicar y guardar, la siguiente corrida toma como punto de partida la mayor versión publicada en landing (el nombre de la carpeta es la prueba de que ese rango ya se extrajo). Queda registrado en `ingestion_log.mode_reason`.
- **Entre landing y bronze** el checkpoint de Auto Loader registra qué archivos ya se procesaron: si la tarea falla y se reanuda, no duplica ni pierde archivos.
- **Pruebas realizadas (29/09/2026):**
  - La tarea de bronze falló a mitad de carga y, al reanudar, bronze quedó con exactamente las filas de la fuente.
  - Con el parámetro de prueba `simulate_failure = Orders`, `Orders` falló después de publicar su lote y antes de guardar su versión, y serverless reintentó la tarea. En el reintento las otras tablas extrajeron 0 filas; en la corrida siguiente `Orders` recuperó la versión desde landing y extrajo 0 filas.
  - En ninguna tabla de bronze hay dos filas con la misma PK, `_ct_version` y `_ct_operation`.
- **Descartado (versión anterior de este diseño): carpeta por `run_id` con `overwrite` y la marca de agua como única fuente de verdad.** Un reintento de la tarea sobrescribía el lote ya publicado de las tablas que habían terminado, con lo que se perdían cambios que bronze aún no había leído. Además, una caída entre escribir y guardar la marca provocaba que el mismo cambio se extrajera dos veces.
- **Lo que sí puede repetirse, y es correcto:** si una fila cambia mientras se lee el snapshot inicial, llega en el snapshot con la versión de corte y otra vez en el incremental con su versión real. No es un duplicado: son dos registros de cambio distintos. Silver toma la última versión por PK, que es lo que corresponde para el estado actual (SCD1) de todas formas.
- **Descartado: snapshot isolation en una transacción con la lectura de la versión.** Es la forma exacta que documenta Microsoft, pero Spark JDBC abre una conexión por consulta y no comparte transacción entre la versión y el SELECT. El caso anterior es la única consecuencia y silver lo absorbe.
- **Recarga automática:** si la versión de partida es menor que `CHANGE_TRACKING_MIN_VALID_VERSION` (el extractor no corrió dentro de la retención de 7 días), esa tabla se recarga completa y el motivo queda en `ingestion_log.mode_reason`.
- **Borrados en una recarga completa:** las filas borradas en la fuente mientras no se extraía no llegan por Change Tracking, y sin más quedarían para siempre en silver. Por eso, en una recarga completa, toda clave vigente en bronze que no aparece en el snapshot nuevo se agrega al mismo lote como un DELETE sintético con la versión de corte (`mode_reason` dice cuántos). Supuesto: bronze está al día con lo publicado en landing; el job carga bronze justo después de cada extracción, así que solo fallaría si además quedó un lote sin cargar antes de la recarga.
- **Concurrencia:** `max_concurrent_runs: 1` evita que dos corridas compitan por la misma marca de agua.
- **Fallas parciales:** si una tabla falla, se registra en `ingestion_log` con el error, las demás siguen y la tarea termina en error para que el job la reintente y avise por correo. El registro se escribe tabla por tabla, así que no se pierde si el proceso se interrumpe.

## D-10. Evolución de esquema y trazabilidad

- **Detección en la extracción:** en cada corrida se leen las columnas de `INFORMATION_SCHEMA` y se comparan con las de la extracción anterior (guardadas en `ct_watermarks`). Columnas nuevas o quitadas quedan en `ingestion_log` (`schema_changed`, `added_columns`, `removed_columns`). La extracción siempre usa la lista de columnas vigente, así que una columna nueva viaja sola.
- **Adopción en bronze:** Auto Loader con `schemaEvolutionMode = addNewColumns` y `mergeSchema`. Ante una columna nueva, el stream se detiene, guarda el esquema ampliado y el notebook lo reinicia. Las filas anteriores quedan con la columna en NULL. Datos que no encajan en el esquema van a `_rescued_data` en lugar de perderse.
- **Descartado: fallar ante cualquier cambio de esquema.** Es más estricto, pero detiene toda la ingesta por una columna nueva que no rompe nada. Una columna quitada o un cambio de tipo sí deben revisarse: quedan registrados en el log y el contrato se valida en silver.
- **Trazabilidad por fila:** `_source` (base, esquema y tabla de origen), `_batch_id` (corrida del job), `_ct_version` y `_ct_operation` (`S` snapshot, `I`, `U`, `D`), `_extracted_at`, `_ingested_at` y `_source_file` (archivo exacto de landing). Con eso se responde cuándo y de dónde llegó cada dato, y se une con `ingestion_log` por `batch_id`.
- **Bronze es la fuente tal cual:** las columnas de origen conservan su nombre y su valor, sin limpieza, filtros ni deduplicación. Los tipos se traducen sin pérdida (NVARCHAR → string, DECIMAL → decimal con la misma precisión, DATETIME2 → timestamp). Lo único agregado son las columnas de metadatos con prefijo `_`, que no alteran las de origen. Si llega un dato inválido (un país sucio, una cantidad 0), bronze lo guarda igual; se trata en silver.
- **Bronze append-only:** `delta.appendOnly = true` impide UPDATE y DELETE sobre bronze. Los DELETE de la fuente llegan como filas con `_ct_operation = 'D'` y solo la PK.

## D-11. Orquestación y frecuencia

- **Lakeflow Job** en cómputo serverless: extracción → bronze → pipeline de silver y gold (esta última tarea desde el nivel 2, D-12). Definido en un Declarative Automation Bundle con targets `dev` y `prod`. Cada target fija su catálogo, así que el mismo código se promueve sin cambios.
- **Frecuencia: cada hora.** Change Tracking entrega el estado neto por PK, así que una frecuencia mayor reduce los estados intermedios de `Payments` que no se llegan a ver (D-03). La retención de 7 días da margen amplio ante fallas.
- **Schedule en pausa por defecto** en ambos targets: cada corrida consume DBU, así que se activa a propósito al pasar a producción.
- **Streaming con costo de batch:** bronze usa `trigger(availableNow=True)`. Pasar a tiempo casi real solo requiere cambiar el trigger a `processingTime` y ejecutar el stream de forma continua; la lógica, los checkpoints y las tablas no cambian.

## D-12. Transformación con Lakeflow Declarative Pipelines

- **Decisión:** un solo pipeline declarativo (`andina_transform`), serverless y en modo *triggered*, con silver y gold. El job lo ejecuta como tercera tarea, después de bronze. El diagrama y la responsabilidad de cada etapa están en [modelo_datos.md](modelo_datos.md).
- **Por qué declarativo:** se escribe qué tabla depende de cuál y qué reglas de calidad cumple; el motor resuelve el orden, el estado de streaming, los reintentos y el cálculo incremental, y registra las métricas de calidad en el event log. Es "calidad como código", no notebooks sueltos.
- **Descartado: notebooks con `MERGE` escritos a mano.** Habría que programar el orden entre tablas, el SCD2, la deduplicación por secuencia y el manejo de DELETE; es más código y más lugares donde equivocarse.
- **Descartado: dbt.** Muy bueno para SQL por lotes, pero sin streaming ni AUTO CDC nativos, y agrega otra herramienta que desplegar. Encaja mejor en un equipo que ya lo usa.
- **Un pipeline para silver y gold, no dos:** el motor ve el grafo completo y refresca gold solo cuando silver cambió. Se separarían si tuvieran dueños o frecuencias distintas.
- **Triggered, no continuo:** los datos llegan cada hora; un pipeline continuo tendría cómputo encendido todo el tiempo sin beneficio.
- **Entornos:** el pipeline recibe el catálogo por configuración (`andina.catalog`) y el bundle lo fija por target. Agregar `staging` es un target más en `databricks.yml` con su catálogo `andina_staging`; no cambia el código.

## D-13. Cambios en el tiempo: AUTO CDC con SCD1 y SCD2

- **Estado actual (SCD1)** para todas las entidades: AUTO CDC aplica cada cambio de bronze sobre su clave, ordenado por `_ct_version`, y borra cuando llega un DELETE. Esto resuelve también lo que D-09 deja a silver: si el mismo cambio llegara dos veces, se aplica una sola vez.
- **Historial (SCD2) solo donde el negocio lo necesita:** `customer_segment_history` (el reto lo pide para el segmento; sirve para analizar ventas con el segmento de ese momento) y `payment_status_history` (el pago cambia después de creado y su recorrido importa para detectar demoras y reintentos). `track_history_column_list` hace que solo esas columnas abran versiones nuevas.
- **Descartado: SCD2 de todas las columnas.** Una mudanza o un cambio de teléfono abriría versiones que nadie consulta y harían crecer la dimensión sin valor analítico.
- **Orden por versión de Change Tracking, no por `UpdatedAt`:** la versión es el orden real de la fuente; `UpdatedAt` depende de que la aplicación la mantenga (D-03). En SCD2, `sequence_by` es `struct(_ct_version, updated_at)`: ordena por versión y deja la fecha de negocio en `__START_AT`/`__END_AT`.
- **Primera versión conocida:** el historial empieza con la primera ingesta (D-05). En `gold.dim_customer` esa versión se abre en 1900-01-01 para que las ventas anteriores a la ingesta encuentren al cliente; en silver se conserva la fecha real.

## D-14. Calidad de datos: nada se descarta en silencio

Reglas escritas como expectations en las vistas `*_changes`, con una acción según el impacto:

| Acción | Cuándo | Casos |
|---|---|---|
| `fail` (detiene el pipeline) | El dato rompe la estructura y no hay forma segura de seguir | PK nula |
| `reject` (no entra al estado actual) | El registro no tiene sentido de negocio | Línea con cantidad 0 o negativa: se aplica como borrado en `silver.order_items` y se guarda en `silver.rejected_order_items` |
| `warn` (entra y se cuenta) | El dato es usable con una bandera o una corrección documentada | Email inválido, país no reconocido, fecha futura, total negativo, canal desconocido |

- **Cuarentena para huérfanos:** las 25 líneas con `ProductId` inexistente siguen en `silver.order_items` y se listan en `silver.quarantine_order_items`. En gold apuntan al producto `-1` "Producto desconocido", así los totales de venta cuadran con la fuente.
- **Descartado: descartar los huérfanos.** Las ventas totales dejarían de cuadrar sin que nadie lo note, y el problema de origen (la migración legacy) no quedaría visible para corregirlo.
- **Registro único:** `silver.data_quality_issues` tiene una fila por problema con regla, acción, entidad, id y detalle. Complementa las métricas del event log, que dicen cuántos, con el cuál y el por qué.
- **La causa importa:** los 178 pedidos cuyo total no cuadra se separan en 140 descuentos aplicados solo a la cabecera y 38 con una línea de cantidad 0 excluida. Los 68 pedidos sin líneas, en 56 sin líneas en la fuente y 12 que solo tenían líneas en 0.
- **Duplicados de clientes, marcados y no fusionados:** dos reglas (email canónico sin alias `+...`, y nombre + teléfono) detectan las 57 cuentas duplicadas; 47 las detectan ambas, así que ninguna bastaba sola. Solo se usan emails con formato válido: una primera versión agrupaba a 10 clientes con el valor de relleno `sin-correo`. Fusionar cuentas es una decisión de negocio (puede haber homónimos), así que gold expone `is_possible_duplicate` y `principal_customer_id`.
- **Por qué `reject` y no `drop` para la cantidad 0:** con `expect_or_drop`, si una línea válida se actualizaba a 0, el cambio se descartaba y silver conservaba la cantidad anterior, inflando las ventas. Aplicarla como borrado la saca del estado actual en ambos casos (nace en 0 o pasa a 0), y la tabla de rechazos evita además releer todo bronze para reportarlas.
- **Doble cobro:** dos pagos que estuvieron aprobados (según el historial SCD2) para el mismo pedido y monto, con menos de 60 s de diferencia. Detecta los 70 casos; el segundo pago es el que hay que devolver. Se lee del historial y no del estado actual para que el caso no desaparezca cuando el negocio lo devuelve (pasa a Reembolsado); `duplicate_current_status` dice si ya se devolvió.
- **Columnas no mapeadas:** las vistas de silver eligen columnas de forma explícita (un contrato). Una columna nueva de la fuente llega a bronze sola, pero no a silver: `silver.unmapped_source_columns` y la regla `columna_no_mapeada` la hacen visible hasta que se decida llevarla.
- **Sin datos personales en la tabla de problemas:** `detail` describe el error ("sin @", "@ repetida") en lugar de copiar el email. Es una tabla para compartir con quien corrige los datos.
- **Validación de punta a punta:** la última tarea del job comprueba bronze = silver, gold = silver, claves únicas e integridad del modelo, y falla si algo no cuadra (resultado en `ops.validation_log`). El catálogo completo de reglas está en [reglas_calidad.md](reglas_calidad.md).
- **Descartado: corregir en la fuente o en bronze.** Bronze guarda lo recibido (D-10); las correcciones viven en silver y conservan el valor original (`order_date_raw`, `country_raw`, `email`).

## D-15. Modelo dimensional en gold

- **Decisión:** modelo estrella con `dim_date`, `dim_customer`, `dim_product` y tres hechos de grano explícito: `fact_order_lines` (línea), `fact_orders` (pedido) y `fact_payments` (pago).
- **Por qué estrella:** es el modelo que entienden las herramientas de BI y los analistas; las consultas son joins simples de hechos a dimensiones.
- **Descartado: una sola tabla ancha (One Big Table).** Cómoda para un dashboard puntual, pero repite atributos del cliente en cada línea y obliga a reconstruirla completa cuando cambia una dimensión.
- **Descartado: gold relacional (3FN).** Es lo que ya es silver; gold agregaría una copia sin simplificar las consultas.
- **`dim_customer` híbrida:** una fila por versión de segmento (SCD2) con el resto de atributos vigentes (SCD1). Los hechos guardan la clave de la versión vigente a la fecha del pedido (join point-in-time).
- **Claves sustitutas deterministas:** `customer_key = xxhash64(customer_id, versión)`. Como gold se recalcula, una clave secuencial cambiaría en cada corrida; el hash da siempre la misma clave para la misma versión. Se comprobó recalculando: la huella de las claves no cambió.
- **Miembro desconocido** en `dim_product` (`-1`) para las líneas en cuarentena.
- **Gold como vistas materializadas:** se recalculan desde silver (de forma incremental cuando el motor puede), así que son idempotentes por construcción.

## D-16. Formato, particionamiento y organización

- **Delta** en todas las capas.
- **Sin particionamiento por directorios:** con tablas de miles de filas, particionar genera archivos pequeños y empeora el rendimiento; Databricks lo recomienda recién desde ~1 TB por tabla.
- **Liquid clustering:** silver por su clave (AUTO CDC busca por clave en cada `MERGE`) y los hechos por `date_key` (las consultas analíticas filtran por fecha). Las claves de clustering se pueden cambiar sin reescribir la tabla.
- **Nombres:** silver y gold en `snake_case`; bronze conserva los nombres de la fuente (D-10).

## D-17. Identidades y secretos: service principals, Key Vault y mínimo privilegio

Surgió de una revisión de seguridad del nivel 2: el pipeline leía Azure SQL con el administrador del servidor, los secretos vivían en un scope de Databricks y todo corría con el usuario personal.

- **Un service principal por entorno**, también en dev: `sp-andina-dev` y `sp-andina-prod`. El bundle los fija con `run_as` en cada target, así que el job y el pipeline nunca corren con un usuario. Cada SP tiene `ALL PRIVILEGES` **solo sobre su catálogo** y nada sobre el otro: un error en dev no puede tocar prod. Las tablas son del SP que las crea; dev se reconstruyó completo como `sp-andina-dev` para que así fuera.
- **Descartado: SP solo en prod.** Dev correría con permisos distintos a los de prod, y los problemas de permisos aparecerían recién al promover.
- **Usuario de solo lectura en la fuente:** `databricks_reader`, con `db_datareader` y `VIEW CHANGE TRACKING`. Si la credencial se filtra, solo permite leer. El administrador `sqladmin` queda únicamente para el generador de datos, en el `.env` local.
- **Credenciales en Azure Key Vault** (`kv-andina-8346`, con permisos por Azure RBAC), leídas por el secret scope `andina-kv`. Key Vault centraliza los secretos, permite rotarlos sin tocar Databricks y audita cada lectura. Los SP solo tienen `READ` sobre el scope.
- **Key Vault y mínimo privilegio no se reemplazan:** Key Vault protege dónde se guarda la credencial; el usuario de solo lectura limita el daño si igual se filtra. Hacen falta las dos.
- **Permisos del vault con Azure RBAC:** la aplicación AzureDatabricks tiene *Key Vault Secrets User* (solo leer secretos) y el administrador, *Key Vault Secrets Officer* (gestionarlos y rotarlos), ambos limitados a este vault.
- **Descartado: políticas de acceso.** Son el modelo heredado de Key Vault: no se gobiernan junto con el resto de permisos de Azure ni admiten asignaciones con alcance o condiciones. El vault empezó con políticas de acceso porque Microsoft Graph, necesario para resolver la identidad de AzureDatabricks, no respondía desde la red del candidato. Cuando volvió a responder, se asignaron los roles **antes** de activar RBAC, para que Databricks no perdiera el acceso en ningún momento, y se verificó con una corrida completa del job. Las políticas viejas (inactivas con RBAC) se eliminaron después, para que una vuelta accidental al modelo anterior no reactive permisos olvidados.
- **Rol Owner en la suscripción:** el usuario es Owner de su suscripción, lo que es necesario para crear recursos y asignar permisos, pero con RBAC no da acceso a los secretos (hace falta un rol de datos explícito). Estaba asignado dos veces de forma idéntica; se eliminó el duplicado.
- **Siguiente paso, sin contraseñas:** autenticar en Azure SQL con la identidad administrada del Access Connector (una *service credential* de Unity Catalog) y un usuario de Entra ID en la base. Elimina la contraseña por completo; queda fuera del alcance del reto por tiempo.
- **Datos personales fuera de gold:** `gold.dim_customer` no tiene nombre, email ni teléfono (la analítica no los necesita). Quedan en silver, con máscara de columna (D-25). En gold se prefirió quitar las columnas antes que enmascararlas: lo que no está no se puede filtrar.
- **Firewall de Azure SQL:** se usa una regla temporal por IP para tareas administrativas y se borra al terminar. El rango del ISP y el acceso desde servicios de Azure siguen siendo la mayor exposición; en producción se reemplazan por Private Endpoint (D-02).

## D-18. Capa analítica agregada y AI/BI Dashboard

- **Sí se construye una capa agregada** (`gold.agg_*`, cuatro vistas materializadas en el mismo pipeline). Cada KPI queda definido una sola vez, en código versionado; el dashboard solo lee. Sin esta capa, la definición de "venta neta" viviría repetida en la consulta de cada gráfico y tarde o temprano dos gráficos dirían cosas distintas. Detalle en [kpis.md](kpis.md).
- **Medidas aditivas:** las tablas guardan sumas y conteos; las tasas se calculan como cociente de sumas en la consulta. Así el total de varios países o meses es correcto (promediar porcentajes no lo es).
- **Recompra solo de clientes realmente nuevos:** el historial empieza en octubre de 2024 y 1.846 clientes se registraron antes. Para ellos la primera compra observada no es la primera real (censura por la izquierda), y las primeras cohortes mostraban 75-80 % de recompra con clientes de ~500 días de antigüedad. Las cohortes solo incluyen personas registradas desde el inicio del historial, y el gráfico muestra cohortes de al menos 30 personas. Se detectó al revisar las capturas del dashboard: un número "demasiado bueno" al inicio de una serie es una señal a investigar, no a celebrar.
- **Descartado: consultar directo los hechos desde el dashboard.** Funciona con este volumen, pero repite la lógica en cada gráfico y no se puede validar. Los agregados sí: la tarea de validación comprueba en cada corrida que cuadren con los hechos.
- **Descartado por ahora: metric views de Unity Catalog.** Definen medidas y dimensiones en YAML y las exponen a cualquier herramienta, que es la evolución natural de esta capa. Se eligieron vistas materializadas porque el pipeline ya las gestiona, se prueban igual que el resto y no dependen de una función más reciente.
- **AI/BI Dashboard sobre un SQL warehouse serverless:** nativo de Databricks, respeta los permisos de Unity Catalog del lector (`embed_credentials: false`), se versiona como JSON y se despliega con el bundle a cada entorno con su catálogo. El warehouse se enciende al abrir el dashboard y se apaga solo.
- **Descartado: Power BI.** Es la herramienta habitual en muchas empresas y se conecta bien a Databricks, pero requiere licencias, un archivo `.pbix` fuera del repositorio y una puerta de enlace o conexión adicional. Para el reto no aporta frente a la opción nativa; en una empresa que ya usa Power BI, leería las mismas tablas `agg_*`.
- **Los cuatro KPIs** cubren el negocio de punta a punta: cuánto se vende (ventas netas y ticket), si los clientes vuelven (recompra, la base del modelo del nivel 4), si se cobra bien (aprobación y dobles cobros, que conectan con los tickets urgentes) y cuánto se devuelve (calidad del catálogo).

## D-19. Feature store en Unity Catalog con fotos semanales point-in-time

- **Decisión:** tablas de features en Unity Catalog con el cliente de Feature Engineering: `ml.customer_features` (clave `customer_id` + `as_of_ts` como clave de tiempo) y `ml.ticket_features` (clave `ticket_id`). Guía de uso en [feature_store.md](feature_store.md).
- **Por qué Unity Catalog:** las features quedan gobernadas como cualquier tabla (permisos, linaje hacia silver), y un modelo registrado con `fe.log_model` sabe qué features usa: al puntuar no hay que recalcular nada ni arriesgar diferencias con el entrenamiento.
- **Fotos semanales con clave de tiempo**, cada una calculada solo con hechos anteriores a su fecha, y `timestamp_lookup_key` al entrenar. Garantiza que ninguna observación vea el futuro.
- **Descartado: una tabla con el estado actual de cada cliente.** Es lo más simple, pero al entrenar con observaciones pasadas usaría features calculadas con datos posteriores: fuga del futuro, y el modelo parecería mejor de lo que es.
- **Descartado: fotos diarias.** Más frescas, pero 7 veces más filas para un negocio donde la recompra se mide en meses. La semana es suficiente; la recencia exacta se puede calcular al puntuar si hiciera falta.
- **Segmento del CRM excluido como feature:** su historial empieza con la primera ingesta (D-05); antes, solo existe el segmento de hoy, calculado con compras posteriores. Usarlo sería fuga del futuro.
- **Pagos rechazados desde el historial de estados** (SCD2) y no desde el estado actual: un pago pendiente a la fecha de la foto pudo rechazarse días después.
- **Features por persona, no por cuenta:** las 57 cuentas duplicadas detectadas en silver pertenecen a personas que ya tienen otra cuenta. Las features suman pedidos, tickets y pagos de todas las cuentas de la persona (y la antigüedad cuenta desde su primer registro); la clave sigue siendo `customer_id`, así que cada cuenta recibe las features de su persona y quien consume no cambia nada. En el entrenamiento de recompra cada persona aparece una sola vez por observación, con su cuenta principal: antes, alguien con dos cuentas pesaba doble y cada cuenta veía solo parte de su historia.

## D-20. Modelos, validación y registro con MLflow

- **Propensión de recompra:** probabilidad de que un cliente con compras vuelva a comprar en los próximos 90 días (el mismo horizonte que el KPI de recompra). `HistGradientBoostingClassifier`: maneja nulos (clientes sin compras en la ventana) y relaciones no lineales sin preprocesar; con miles de filas no hace falta nada distribuido.
- **Tickets urgentes:** TF-IDF del asunto y del cuerpo, más el historial del cliente antes del ticket, con regresión logística balanceada (las urgentes son minoría). El texto es la señal principal, porque el agente sube la prioridad cuando el cliente expresa urgencia. Hay un ~8 % de ruido de etiquetado en la fuente: el techo del modelo está por debajo de la perfección.
- **Validación temporal, no aleatoria:** se entrena con el pasado y se evalúa con meses posteriores. Un split aleatorio pone observaciones del futuro en el entrenamiento e infla las métricas.
- **Línea base:** el modelo de recompra se compara con una regla sin modelo (ordenar por recencia). Si no la supera claramente, el modelo no aporta.
- **La probabilidad, no la clase:** los modelos se registran como `pyfunc` que devuelve la probabilidad. Para ordenar clientes o una cola de tickets se necesita el puntaje; el umbral lo decide el negocio según su capacidad (cupones, agentes).
- **Registro en Unity Catalog** (`ml.repurchase_propensity`, `ml.urgent_ticket_classifier`) con el alias `champion`. La puntuación siempre usa `@champion`; promover una versión nueva es mover el alias, sin cambiar código. Cada versión queda ligada a su corrida de MLflow (parámetros, métricas) y a las tablas de features con las que se entrenó.
- **Promoción con control (`challenger` → `champion`):** cada versión nueva queda como `challenger`. Solo pasa a `champion` si su ROC AUC iguala o supera al del champion actual **evaluado en el mismo conjunto de prueba** (con `score_batch`, que busca las features con la misma lógica point-in-time). Comparar contra las métricas guardadas del champion no sirve, porque se midieron con otro periodo. Primera corrida con la regla: la v3 de recompra (0,8204) no superó a la v2 (0,8222) y no se promovió; la v3 de tickets empató (0,8541) y sí.
- **Descartado: promover siempre la última versión.** Era el comportamiento inicial: un reentrenamiento peor habría reemplazado al modelo en uso sin que nadie lo notara.
- **Validación de ML en cada corrida** (`src/ml/05_validar_ml.py`): point-in-time fila por fila en tres fotos, ninguna foto antes del alta, claves únicas, frescura, alias `champion` presentes, champion de recompra mejor que la línea base, puntajes válidos y completos. Falla el job si algo no cuadra.

## D-21. Puntuación y servicio

- **Batch diario** con `fe.score_batch` en el job `andina_ml` (features → entrenamiento → puntuación): `ml.repurchase_scores` y `ml.urgent_ticket_scores`. Es suficiente para campañas de retención y para ordenar la cola de soporte cada mañana.
- **Reentrenamiento en el mismo job, por simplicidad.** En producción se separaría: las features y la puntuación a diario, el reentrenamiento semanal o cuando se degrade el modelo, y la promoción a `champion` solo si la versión nueva supera a la actual en la evaluación temporal.
- **Baja latencia, diseñada y no desplegada:** tabla online sincronizada desde `ml.customer_features` + Model Serving del modelo `@champion` (que busca las features solo, porque se registró con `fe.log_model`). No se desplegó porque cuesta mientras está encendido y el caso de uso actual es batch.

## D-22. Documentos y chunking del RAG

- **Corpus generado con IA y coherente con los datos:** 11 documentos (políticas de devolución, envíos y garantías; FAQs de pagos y de cuenta; programa de clientes; atención al cliente; manuales por categoría) en `rag_docs/`. Usan los mismos métodos de pago por canal, las mismas reglas de segmento del CRM, los mismos tipos de ticket y prioridades, y las mismas categorías y marcas que la base. A eso se suma un chunk por producto del catálogo (`silver.products`).
- **Documentos en un Volume de Unity Catalog** (`genai.docs`): el repositorio es la fuente versionada y el Volume la copia gobernada (permisos, linaje). Un documento borrado del repositorio se borra del Volume y sus chunks del índice.
- **Chunking por sección (`##`), con el título del documento y de la sección al inicio de cada chunk.** Cada sección responde una pregunta concreta ("plazos de reembolso por método de pago"); el título da contexto al embedding. Las secciones de más de 1.200 caracteres se dividen por párrafos, con un párrafo de solapamiento.
- **Descartado: chunks de tamaño fijo (por ejemplo, 500 tokens).** Parten tablas y listas a la mitad y mezclan dos temas en un chunk, lo que empeora tanto el retrieval como la cita de la fuente.
- **Un chunk por producto**, con nombre, SKU, categoría, marca, precio, estado (disponible o descontinuado) y descripción en texto: las preguntas de catálogo se responden con datos reales y actualizados.

## D-23. Embeddings, índice y actualización

- **Embeddings: se eligió `databricks-qwen3-embedding-0-6b` (multilingüe) y se usa `databricks-gte-large-en`.** El modelo multilingüe es lo correcto para documentos y preguntas en español, y la primera corrida lo confirmó (recall@5 de 1,0 frente a 0,87-0,93 con gte). Pero su endpoint dejó de estar disponible en el workspace después de esa corrida. Se cambió a `gte-large-en` (admite textos más largos que `bge-large-en`) con búsqueda híbrida para compensar con palabras exactas; volver a un modelo multilingüe estable es la mejora inmediata.
- **Generación con `databricks-meta-llama-3-3-70b-instruct`:** los endpoints de Claude figuran en el workspace con cuota 0. Llama 3.3 responde bien en español, devuelve texto plano y soporta tool calling para el nivel 6.
- **Lección:** que un endpoint aparezca en la lista no garantiza que esté disponible. La ingesta ahora verifica que la última sincronización del índice no haya fallado, además del conteo de filas.
- **Mejoras guiadas por la evaluación:** linealizar las tablas de Markdown (cada fila como frase) y usar el vocabulario del cliente subieron el recall@5 híbrido de 0,867 a 0,933. Detalle en [rag.md](rag.md).
- **Vector Search con índice Delta Sync** sobre `genai.doc_chunks` (con Change Data Feed) y embeddings gestionados: Databricks calcula los embeddings al sincronizar, sin código propio de embeddings.
- **Actualización incremental e idempotente:** la tabla de chunks se actualiza con `MERGE` por hash del contenido (solo cambia lo que cambió, y se borran los chunks de secciones eliminadas), y el índice en modo *triggered* procesa solo esos cambios al sincronizar. El job `andina_rag` corre a diario (después del de ML, porque el catálogo puede cambiar) o cuando se actualizan documentos.
- **SDK de Databricks en lugar del paquete `databricks-vectorsearch`:** instalar ese paquete bajaba la versión de protobuf y rompía el entorno serverless antes de arrancar.
- **Costo:** el endpoint de Vector Search se cobra por hora mientras exista, a diferencia del resto del proyecto. Se creó solo en dev y se borró después de medir (el job lo recrea solo cuando se necesite).

## D-24. Evaluación del retrieval y trazabilidad

- **Golden set de 30 preguntas** redactadas como las haría un cliente (no copiadas del documento), cada una con la sección o el producto que la responde; algunas admiten más de una fuente válida.
- **Métricas:** recall@1, @3 y @5 y MRR, comparando búsqueda vectorial (ANN) e híbrida (vectorial + palabras clave). Se guardan en `genai.retrieval_eval_summary` y en MLflow, y cada pregunta con su rango en `genai.retrieval_eval`, para ver qué falla.
- **Umbral de calidad:** si el recall@5 cae por debajo de 0,80 (por un cambio de documentos o de chunking), la tarea falla en lugar de dejar en uso un índice peor.
- **Trazabilidad de cada respuesta:** las respuestas generadas citan las fuentes ([n] → documento y sección), y cada chunk guarda su `source` (archivo del Volume o tabla de silver). Unity Catalog da el linaje Volume → `genai.doc_chunks` → índice.
- **Generación acotada:** el modelo responde solo con los fragmentos recuperados y, si no alcanzan, lo dice y deriva al chat. Es la primera defensa contra respuestas inventadas, pero no basta: en un ejemplo el modelo leyó mal un fragmento correcto (costo de envío a Arequipa). Por eso el nivel 6 usa herramientas deterministas para los cálculos y se propone evaluar la fidelidad de las respuestas con un juez automático.
- **Búsqueda híbrida en uso:** con `gte-large-en`, la híbrida supera a la vectorial pura en recall@5 y MRR, porque las palabras exactas compensan lo que el modelo en inglés no capta del español.

## D-25. Máscaras de columna para datos personales y control de costos

- **Máscaras de columna de Unity Catalog** en `silver.customers`: `first_name` y `last_name` (`A***`), `email` y `email_norm` (`***@dominio`) y `phone` (`*** 321`). Las funciones viven en `<catalog>.ops` (`mask_name`, `mask_email`, `mask_phone`) y las declara el propio pipeline en el esquema de la tabla, así que se aplican igual en dev y prod y quedan versionadas.
- **Quién ve el valor real:** solo el grupo `andina-pii-readers`, cuyos miembros son los service principals del pipeline. Los necesitan: la detección de cuentas duplicadas compara emails y teléfonos reales. Se verificó en los dos sentidos: un usuario fuera del grupo ve los datos enmascarados, y tras recalcular desde cero la detección siguió encontrando exactamente las 57 cuentas duplicadas (si el pipeline viera `***@gmail.com`, agruparía a cientos de clientes distintos).
- **Descartado: proteger silver solo con permisos de tabla.** Quien necesita silver para analizar segmentos o calidad no necesita ver nombres ni emails; con la máscara puede leer la tabla sin exponerlos.
- **Descartado: una vista enmascarada aparte.** Duplica objetos y deja la tabla base expuesta a quien tenga permiso sobre ella; la máscara protege la tabla misma, la consulte quien la consulte.
- **Grupo local del workspace** (`is_member`): crear grupos a nivel de cuenta requiere ser administrador de la cuenta de Databricks. En producción sería un grupo de cuenta sincronizado desde Entra ID y la función usaría `is_account_group_member`.
- **Alerta de presupuesto** `andina-reto-mensual` en la suscripción: 20 USD al mes, con avisos por correo al 50 % y al 90 % del gasto real y al 100 % del proyectado. La suscripción no tiene límite de gasto, y el único costo continuo (el endpoint de Vector Search) se apaga cuando no se usa.
