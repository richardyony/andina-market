# Andina Market: reto técnico AI Data Engineer

Plataforma de datos de punta a punta sobre Azure y Databricks: la ingesta desde Azure SQL alimenta un lakehouse medallion en Unity Catalog, y de ahí salen la analítica, el feature store, RAG y los agentes.

> **Estado:** en construcción. Niveles 0 y 1 completos: base de origen con datos sintéticos, ingesta incremental hasta bronze desplegada con un bundle, diagrama de arquitectura y diseños de streaming y SAP. Esta sección se actualiza por nivel.

| Nivel | Alcance | Estado |
|---|---|---|
| 0. Fuente | Azure SQL, datos sintéticos, Change Tracking | ✅ |
| 1. Ingesta | JDBC incremental (CT) → landing → Auto Loader → bronze; DABs; diseño streaming y SAP | ✅ |
| 2. Transformación | Lakeflow Declarative Pipelines, SCD1/SCD2, cuarentena | ⏳ |
| 3. Gold + BI | Modelo estrella, dashboard AI/BI | ⏳ |
| 4. Feature Store | Features point-in-time, MLflow en UC | ⏳ |
| 5. RAG | Vector Search, evaluación de retrieval | ⏳ |
| 6. Agente | Diseño o agente mínimo | ⏳ |

## Estructura

```
source_db/          DDL de Azure SQL (esquema, FK legacy, Change Tracking)
data_generator/     Generador de histórico, simulador de cambios, clickstream
sample_data/        Muestra de eventos de clickstream (.jsonl)
databricks.yml      Bundle (Declarative Automation Bundle): targets dev y prod
resources/          Definición de los jobs del bundle
src/ingesta/        Notebooks de ingesta: extracción con CT → landing, Auto Loader → bronze
docs/               Decisiones de arquitectura y documentación de datos
```

- [Arquitectura: diagramas, infraestructura, permisos y costos](docs/arquitectura.md)
- [Registro de decisiones](docs/decisiones.md)
- [Datos sintéticos y casos borde](docs/datos_sinteticos.md)
- [Diseño: clickstream en tiempo real (Event Hubs + Structured Streaming)](docs/diseno_streaming.md)
- [Diseño: integración de SAP ECC on-premise (ADF + SHIR + SAP CDC)](docs/diseno_sap.md)

## Cómo reproducir: base de origen

### 1. Azure SQL Database

1. Crea un servidor lógico y la base `andina_oltp` con la **oferta gratuita** (serverless, General Purpose), idealmente en la misma región del workspace. Activa la auto-pausa. En este reto el servidor es `sql-andina-cus`, en Central US, porque East US 2 no aceptaba servidores nuevos (ver [D-06](docs/decisiones.md)).
2. En **Redes** del servidor:
   - agrega la IP de tu PC;
   - activa *Permitir que los servicios y recursos de Azure accedan a este servidor*. Es necesario porque el cómputo serverless de Databricks no tiene IP fija. En producción se reemplaza por Private Endpoint.

### 2. Tu PC (Windows)

1. Instala Python 3.11 o superior y el [Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/es-es/sql/connect/odbc/download-odbc-driver-for-sql-server).
2. En PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env      # y completa servidor, usuario y clave
```

### 3. Generar y cargar

```powershell
# Revisión sin tocar la base: escribe CSV en ./out y muestra la volumetría y los casos borde
python -m data_generator.load_initial --dry-run

# Carga real: crea el esquema, inserta el histórico, agrega la FK legacy y activa Change Tracking
python -m data_generator.load_initial --reset
```

Si la base está en auto-pausa, el primer intento puede fallar con el error 40613. El script reintenta solo.

### 4. Verificar en el Query editor del portal

```sql
SELECT 'Customers' t, COUNT(*) n FROM dbo.Customers UNION ALL
SELECT 'Orders', COUNT(*) FROM dbo.Orders UNION ALL
SELECT 'Payments', COUNT(*) FROM dbo.Payments;
SELECT OBJECT_NAME(object_id) tabla FROM sys.change_tracking_tables;
SELECT CHANGE_TRACKING_CURRENT_VERSION() version_actual;
```

### 5. Simular operación, después de la primera ingesta

```powershell
python -m data_generator.simulate_changes                  # un día de cambios
python -m data_generator.simulate_changes --schema-change  # + columna nueva en Orders
```

## Cómo reproducir: ingesta (nivel 1)

Diagramas completos en [docs/arquitectura.md](docs/arquitectura.md). Resumen:

```
Azure SQL (andina_oltp)                     Unity Catalog: <catalog> = andina_dev | andina_prod
 dbo.* + Change Tracking
        │  01_extract_sqlserver_ct (JDBC)
        │  full la 1.ª vez / CHANGETABLE después
        ▼
 /Volumes/<catalog>/landing/sqlserver/<tabla>/<batch_id>/*.parquet
        │  02_bronze_autoloader (Auto Loader, availableNow, checkpoint)
        ▼
 <catalog>.bronze.<tabla>   Delta append-only + _ct_version, _ct_operation, _batch_id,
                            _extracted_at, _ingested_at, _source, _source_file
 <catalog>.ops.ct_watermarks · <catalog>.ops.ingestion_log   (control y trazabilidad)
```

### 1. Prerrequisitos en Databricks (una vez)

- Catálogos `andina_dev` y `andina_prod` con los esquemas `landing`, `bronze`, `silver`, `gold`, `ml`, `genai` y `ops`, sobre una ubicación externa ADLS Gen2 ([D-07](docs/decisiones.md)).
- Secret scope `andina-sql` con las claves `server`, `database`, `user` y `password`.
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) autenticada contra el workspace (este proyecto usa `auth_type = azure-cli`).

Los Volumes y las tablas de `ops` los crea el propio job si no existen.

### 2. Desplegar y ejecutar

```powershell
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run andina_ingesta -t dev     # 1.ª vez: carga completa
python -m data_generator.simulate_changes --schema-change
databricks bundle run andina_ingesta -t dev     # incremental + columna nueva
```

El job tiene un schedule horario que queda en pausa en ambos targets: se activa a propósito al promover a producción.

### 3. Verificar

```sql
-- Qué se extrajo en cada lote, en qué modo y si cambió el esquema
SELECT batch_id, source_table, mode, from_version, to_version, rows_extracted, schema_changed, added_columns, status
FROM andina_dev.ops.ingestion_log ORDER BY started_at DESC;

-- Historial de un pedido en bronze: snapshot inicial (S) y sus cambios (I/U/D)
SELECT OrderId, Status, _ct_version, _ct_operation, _batch_id, _ingested_at
FROM andina_dev.bronze.orders WHERE OrderId = 26408 ORDER BY _ct_version;
```

Resultado de la prueba del 29/09/2026: la carga completa dejó en bronze exactamente las 116.925 filas de la fuente, sin duplicados, aun después de una falla y reanudación de la tarea de bronze. El incremental trajo 774 cambios netos (incluidos 6 DELETE de tickets) y la columna nueva `Orders.CouponCode`, detectada en `ingestion_log` y agregada a bronze sin intervención.

## Uso de IA

Construido con Claude como asistente, que propuso código y documentación bajo mi dirección y revisión. Detalle por componente:

- **Generador de datos, DDL y documentación inicial:** generados con Claude, y revisados y ajustados por mí.
- **Infraestructura de Azure y Unity Catalog:** Claude ejecutó los comandos de Azure CLI y Databricks CLI bajo mi aprobación paso a paso. Las incidencias (regiones bloqueadas, IP dinámica) y sus decisiones están en D-06 y D-07.
- **Ingesta del nivel 1 (notebooks y bundle):** generados con Claude a partir del diseño del plan del proyecto (Change Tracking, landing en Parquet, bronze append-only). Claude también ejecutó la prueba de punta a punta; los resultados se verifican con las consultas de la sección anterior.
- **Diagramas y diseños de streaming y SAP:** redactados con Claude. Las cifras del clickstream (duplicados, retrasos, anónimos) salen de analizar la muestra real; las decisiones y alternativas deben poder defenderse en la entrevista, así que conviene revisarlas.
- *(Se completa por nivel.)*
