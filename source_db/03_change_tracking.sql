/* =====================================================================
   Change Tracking (CT) - mecanismo de detección de cambios para la ingesta
   ---------------------------------------------------------------------
   Por qué CT y no CDC ni watermark por UpdatedAt:
   - Watermark por UpdatedAt no detecta DELETEs y depende de que la app
     siempre actualice la columna (no está garantizado en un legacy).
   - CDC guarda cada transición intermedia en tablas de historial y usa el
     SQL Agent/scheduler interno: más costo y más almacenamiento en la fuente.
   - CT es liviano, síncrono, sin historial en la fuente, y devuelve la
     operación neta (I/U/D) por PK desde una versión dada. El historial lo
     construimos nosotros en bronze (append-only).
   Trade-off asumido: si una fila cambia varias veces entre dos extracciones,
   solo vemos el estado final. Lo mitigamos con la frecuencia de extracción
   (ver README, sección "Payments").

   Retención de 7 días: si el extractor no corre en ese período, la versión
   guardada queda por debajo de CHANGE_TRACKING_MIN_VALID_VERSION y el
   extractor hace automáticamente una recarga completa de esa tabla.
   ===================================================================== */

-- Snapshot isolation: la extracción inicial (snapshot + versión) debe ser
-- consistente. En Azure SQL suele estar activo; es idempotente.
ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID())
    ALTER DATABASE CURRENT
    SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 7 DAYS, AUTO_CLEANUP = ON);
GO

DECLARE @t SYSNAME, @sql NVARCHAR(400);
DECLARE c CURSOR LOCAL FAST_FORWARD FOR
    SELECT v FROM (VALUES (N'Customers'), (N'Products'), (N'Orders'),
                          (N'OrderItems'), (N'Payments'), (N'SupportTickets')) AS x(v);
OPEN c;
FETCH NEXT FROM c INTO @t;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_tables
                   WHERE object_id = OBJECT_ID(N'dbo.' + @t))
    BEGIN
        SET @sql = N'ALTER TABLE dbo.' + QUOTENAME(@t) +
                   N' ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);';
        EXEC sp_executesql @sql;
    END
    FETCH NEXT FROM c INTO @t;
END
CLOSE c; DEALLOCATE c;
GO

-- Verificación
SELECT OBJECT_NAME(object_id) AS tabla,
       CHANGE_TRACKING_MIN_VALID_VERSION(object_id) AS min_valid_version
FROM sys.change_tracking_tables;
SELECT CHANGE_TRACKING_CURRENT_VERSION() AS version_actual;
GO

/* ---------------------------------------------------------------------
   Usuario de solo lectura para Databricks (principio de mínimo privilegio).
   Ejecutar conectado a la base andina_oltp (usuario contenido, sin login
   en master). Cambia la contraseña y guárdala en un secret scope.
   VIEW CHANGE TRACKING es necesario para leer CHANGETABLE().
   --------------------------------------------------------------------- */
-- CREATE USER databricks_reader WITH PASSWORD = '<<cambia-esta-clave>>';
-- ALTER ROLE db_datareader ADD MEMBER databricks_reader;
-- GRANT VIEW CHANGE TRACKING ON SCHEMA::dbo TO databricks_reader;
