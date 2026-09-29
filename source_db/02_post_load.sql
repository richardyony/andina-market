/* =====================================================================
   Post-carga: FK OrderItems -> Products como NO CONFIABLE (WITH NOCHECK).
   Simula una migración legacy: hay líneas de pedido cuyo ProductId no
   existe en el catálogo actual. La FK protege las inserciones nuevas,
   pero no valida las filas históricas -> huérfanos reales que el
   lakehouse debe detectar y enviar a cuarentena.
   ===================================================================== */
IF OBJECT_ID(N'dbo.FK_OrderItems_Products', N'F') IS NULL
    ALTER TABLE dbo.OrderItems WITH NOCHECK
    ADD CONSTRAINT FK_OrderItems_Products FOREIGN KEY (ProductId)
        REFERENCES dbo.Products(ProductId);
GO

-- is_not_trusted = 1 confirma que hay filas históricas sin validar
SELECT name, is_not_trusted FROM sys.foreign_keys
WHERE name = N'FK_OrderItems_Products';
GO
