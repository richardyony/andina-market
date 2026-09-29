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
