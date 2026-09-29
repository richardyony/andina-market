/* =====================================================================
   Andina Market - Base transaccional de origen (Azure SQL Database)
   ---------------------------------------------------------------------
   Script re-ejecutable: BORRA y recrea las tablas.
   Orden: 01_schema.sql -> carga inicial (generador) -> 02_post_load.sql
          -> 03_change_tracking.sql
   (el comando `python -m data_generator.load_initial --reset` hace todo)

   Decisiones de diseño de la fuente (simula un OLTP real "heredado"):
   - PK con IDENTITY, como lo haría la aplicación.
   - CreatedAt / UpdatedAt de auditoría (UTC), mantenidos por la aplicación.
     No son la fuente de verdad para la ingesta incremental (lo es Change
     Tracking), pero sirven como respaldo y para auditoría.
   - FKs declaradas, pero las de OrderItems -> Products quedan NO confiables
     (WITH NOCHECK) por una migración legacy: eso permite huérfanos reales
     que el lakehouse debe detectar.
   - Pocos CHECK constraints a propósito: la fuente es permisiva y la calidad
     se garantiza aguas abajo (expectations en silver).
   - Montos en USD (supuesto documentado en el README).
   ===================================================================== */

SET NOCOUNT ON;
GO

/* ---------- Limpieza (orden inverso de dependencias) ---------- */
DROP TABLE IF EXISTS dbo.SupportTickets;
DROP TABLE IF EXISTS dbo.Payments;
DROP TABLE IF EXISTS dbo.OrderItems;
DROP TABLE IF EXISTS dbo.Orders;
DROP TABLE IF EXISTS dbo.Products;
DROP TABLE IF EXISTS dbo.Customers;
GO

/* ---------- Customers ---------- */
CREATE TABLE dbo.Customers (
    CustomerId   INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Customers PRIMARY KEY,
    FirstName    NVARCHAR(80)  NOT NULL,
    LastName     NVARCHAR(80)  NOT NULL,
    Email        NVARCHAR(200) NULL,          -- NULL: clientes de tienda física sin email
    Phone        NVARCHAR(40)  NULL,
    City         NVARCHAR(80)  NULL,
    Country      NVARCHAR(40)  NULL,          -- ISO-2 esperado; hay valores sucios
    Segment      NVARCHAR(20)  NOT NULL,      -- Nuevo | Regular | Frecuente | VIP
    SignupDate   DATE          NOT NULL,
    CreatedAt    DATETIME2(3)  NOT NULL CONSTRAINT DF_Customers_CreatedAt DEFAULT SYSUTCDATETIME(),
    UpdatedAt    DATETIME2(3)  NOT NULL CONSTRAINT DF_Customers_UpdatedAt DEFAULT SYSUTCDATETIME()
);
-- Sin UNIQUE en Email a propósito: el sistema legacy permite duplicados.
CREATE INDEX IX_Customers_Email     ON dbo.Customers(Email);
CREATE INDEX IX_Customers_UpdatedAt ON dbo.Customers(UpdatedAt);
GO

/* ---------- Products ---------- */
CREATE TABLE dbo.Products (
    ProductId    INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Products PRIMARY KEY,
    SKU          NVARCHAR(30)   NOT NULL CONSTRAINT UQ_Products_SKU UNIQUE,
    Name         NVARCHAR(200)  NOT NULL,
    Category     NVARCHAR(60)   NOT NULL,
    Subcategory  NVARCHAR(60)   NULL,
    Brand        NVARCHAR(60)   NULL,
    Price        DECIMAL(12,2)  NOT NULL,     -- precio vigente (el histórico vive en OrderItems)
    Description  NVARCHAR(2000) NULL,
    Status       NVARCHAR(20)   NOT NULL,     -- Active | Discontinued
    CreatedAt    DATETIME2(3)   NOT NULL CONSTRAINT DF_Products_CreatedAt DEFAULT SYSUTCDATETIME(),
    UpdatedAt    DATETIME2(3)   NOT NULL CONSTRAINT DF_Products_UpdatedAt DEFAULT SYSUTCDATETIME()
);
CREATE INDEX IX_Products_UpdatedAt ON dbo.Products(UpdatedAt);
GO

/* ---------- Orders ---------- */
CREATE TABLE dbo.Orders (
    OrderId      INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Orders PRIMARY KEY,
    CustomerId   INT            NOT NULL
                 CONSTRAINT FK_Orders_Customers REFERENCES dbo.Customers(CustomerId),
    OrderDate    DATETIME2(3)   NOT NULL,
    Channel      NVARCHAR(10)   NOT NULL,     -- web | app | tienda
    Status       NVARCHAR(20)   NOT NULL,     -- Pendiente | Pagado | Enviado | Entregado | Cancelado | Devuelto
    TotalAmount  DECIMAL(14,2)  NOT NULL,
    CreatedAt    DATETIME2(3)   NOT NULL CONSTRAINT DF_Orders_CreatedAt DEFAULT SYSUTCDATETIME(),
    UpdatedAt    DATETIME2(3)   NOT NULL CONSTRAINT DF_Orders_UpdatedAt DEFAULT SYSUTCDATETIME()
);
CREATE INDEX IX_Orders_CustomerId ON dbo.Orders(CustomerId);
CREATE INDEX IX_Orders_OrderDate  ON dbo.Orders(OrderDate);
CREATE INDEX IX_Orders_UpdatedAt  ON dbo.Orders(UpdatedAt);
GO

/* ---------- OrderItems ---------- */
CREATE TABLE dbo.OrderItems (
    OrderItemId  INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_OrderItems PRIMARY KEY,
    OrderId      INT            NOT NULL
                 CONSTRAINT FK_OrderItems_Orders REFERENCES dbo.Orders(OrderId),
    ProductId    INT            NOT NULL,     -- FK agregada al final WITH NOCHECK
    Quantity     INT            NOT NULL,
    UnitPrice    DECIMAL(12,2)  NOT NULL,     -- precio al momento de la compra
    CreatedAt    DATETIME2(3)   NOT NULL CONSTRAINT DF_OrderItems_CreatedAt DEFAULT SYSUTCDATETIME(),
    UpdatedAt    DATETIME2(3)   NOT NULL CONSTRAINT DF_OrderItems_UpdatedAt DEFAULT SYSUTCDATETIME()
);
CREATE INDEX IX_OrderItems_OrderId   ON dbo.OrderItems(OrderId);
CREATE INDEX IX_OrderItems_ProductId ON dbo.OrderItems(ProductId);
GO

/* ---------- Payments ---------- */
CREATE TABLE dbo.Payments (
    PaymentId    INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Payments PRIMARY KEY,
    OrderId      INT            NOT NULL
                 CONSTRAINT FK_Payments_Orders REFERENCES dbo.Orders(OrderId),
    Method       NVARCHAR(20)   NOT NULL,     -- tarjeta | transferencia | billetera | efectivo
    Amount       DECIMAL(14,2)  NOT NULL,
    Status       NVARCHAR(20)   NOT NULL,     -- Pendiente | Aprobado | Rechazado | Reembolsado
    PaymentDate  DATETIME2(3)   NOT NULL,     -- momento del intento de pago
    CreatedAt    DATETIME2(3)   NOT NULL CONSTRAINT DF_Payments_CreatedAt DEFAULT SYSUTCDATETIME(),
    UpdatedAt    DATETIME2(3)   NOT NULL CONSTRAINT DF_Payments_UpdatedAt DEFAULT SYSUTCDATETIME()
);
CREATE INDEX IX_Payments_OrderId   ON dbo.Payments(OrderId);
CREATE INDEX IX_Payments_UpdatedAt ON dbo.Payments(UpdatedAt);
GO

/* ---------- SupportTickets ---------- */
CREATE TABLE dbo.SupportTickets (
    TicketId     INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SupportTickets PRIMARY KEY,
    CustomerId   INT            NOT NULL
                 CONSTRAINT FK_SupportTickets_Customers REFERENCES dbo.Customers(CustomerId),
    OrderId      INT            NULL
                 CONSTRAINT FK_SupportTickets_Orders REFERENCES dbo.Orders(OrderId),
    Channel      NVARCHAR(20)   NOT NULL,     -- email | chat | telefono | app
    Subject      NVARCHAR(200)  NOT NULL,
    Body         NVARCHAR(MAX)  NULL,         -- texto libre (puede venir vacío)
    Priority     NVARCHAR(10)   NOT NULL,     -- Baja | Media | Alta | Urgente (asignada por el agente)
    Status       NVARCHAR(20)   NOT NULL,     -- Abierto | EnProceso | Resuelto | Cerrado
    CreatedAt    DATETIME2(3)   NOT NULL,
    UpdatedAt    DATETIME2(3)   NOT NULL CONSTRAINT DF_SupportTickets_UpdatedAt DEFAULT SYSUTCDATETIME()
);
CREATE INDEX IX_SupportTickets_CustomerId ON dbo.SupportTickets(CustomerId);
CREATE INDEX IX_SupportTickets_UpdatedAt  ON dbo.SupportTickets(UpdatedAt);
GO
