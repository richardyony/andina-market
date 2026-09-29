# Andina Market: reto técnico AI Data Engineer

Plataforma de datos de punta a punta sobre Azure y Databricks: la ingesta desde Azure SQL alimenta un lakehouse medallion en Unity Catalog, y de ahí salen la analítica, el feature store, RAG y los agentes.

> **Estado:** en construcción. Por ahora están la base de origen, los datos sintéticos y la muestra de clickstream. Esta sección se actualiza por nivel.

| Nivel | Alcance | Estado |
|---|---|---|
| 0. Fuente | Azure SQL, datos sintéticos, Change Tracking | ✅ |
| 1. Ingesta | JDBC incremental (CT) → landing → Auto Loader → bronze; DABs | ⏳ |
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
docs/               Decisiones de arquitectura y documentación de datos
```

- [Registro de decisiones](docs/decisiones.md)
- [Datos sintéticos y casos borde](docs/datos_sinteticos.md)

## Cómo reproducir: base de origen

### 1. Azure SQL Database

1. Crea un servidor lógico y la base `andina_oltp` con la **oferta gratuita** (serverless, General Purpose) en la misma región del workspace. Activa la auto-pausa.
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

## Uso de IA

Construido con Claude como asistente, que propuso código y documentación bajo mi dirección y revisión. Detalle por componente:

- **Generador de datos, DDL y documentación inicial:** generados con Claude, y revisados y ajustados por mí.
- *(Se completa por nivel.)*
