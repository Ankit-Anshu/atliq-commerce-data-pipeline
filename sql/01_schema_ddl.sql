-- ============================================================================
--  AtliQ Commerce  |  OLTP Schema (Azure SQL Database)
--  Normalized (3NF) operational database that powers the storefront.
--  Run this FIRST, then run the 02..06 insert scripts in order.
-- ============================================================================

-- Optional: run once against the [master] database to create the DB.
-- CREATE DATABASE atliq_commerce;
-- GO
-- USE atliq_commerce;
-- GO

-- ----------------------------------------------------------------------------
-- Clean start (safe re-run). Drop in child -> parent order because of FKs.
-- ----------------------------------------------------------------------------
IF OBJECT_ID('dbo.payments', 'U')     IS NOT NULL DROP TABLE dbo.payments;
IF OBJECT_ID('dbo.order_items', 'U')  IS NOT NULL DROP TABLE dbo.order_items;
IF OBJECT_ID('dbo.orders', 'U')       IS NOT NULL DROP TABLE dbo.orders;
IF OBJECT_ID('dbo.products', 'U')     IS NOT NULL DROP TABLE dbo.products;
IF OBJECT_ID('dbo.customers', 'U')    IS NOT NULL DROP TABLE dbo.customers;
GO

-- ----------------------------------------------------------------------------
-- customers  (one row per registered customer)
-- ----------------------------------------------------------------------------
CREATE TABLE dbo.customers (
    customer_id    INT            IDENTITY(1,1) NOT NULL,
    customer_name  NVARCHAR(100)  NOT NULL,
    email          NVARCHAR(150)  NOT NULL,
    city           NVARCHAR(60)   NOT NULL,
    signup_date    DATE           NOT NULL,
    updated_at     DATETIME2(0)   NOT NULL CONSTRAINT DF_customers_updated DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_customers PRIMARY KEY (customer_id),
    CONSTRAINT UQ_customers_email UNIQUE (email)
);
GO

-- ----------------------------------------------------------------------------
-- products  (product catalog)
-- ----------------------------------------------------------------------------
CREATE TABLE dbo.products (
    product_id    INT            IDENTITY(1,1) NOT NULL,
    product_name  NVARCHAR(120)  NOT NULL,
    category      NVARCHAR(60)   NOT NULL,
    unit_price    DECIMAL(10,2)  NOT NULL,
    updated_at    DATETIME2(0)   NOT NULL CONSTRAINT DF_products_updated DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_products PRIMARY KEY (product_id),
    CONSTRAINT CK_products_price CHECK (unit_price >= 0)
);
GO

-- ----------------------------------------------------------------------------
-- orders  (one row per order; header)
-- updated_at is the WATERMARK column used by the nightly incremental sync.
-- ----------------------------------------------------------------------------
CREATE TABLE dbo.orders (
    order_id      INT            IDENTITY(1,1) NOT NULL,
    customer_id   INT            NOT NULL,
    order_date    DATE           NOT NULL,
    status        NVARCHAR(20)   NOT NULL,   -- Placed / Shipped / Delivered / Cancelled / Returned
    order_amount  DECIMAL(12,2)  NOT NULL,
    created_at    DATETIME2(0)   NOT NULL CONSTRAINT DF_orders_created DEFAULT SYSUTCDATETIME(),
    updated_at    DATETIME2(0)   NOT NULL CONSTRAINT DF_orders_updated DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_orders PRIMARY KEY (order_id),
    CONSTRAINT FK_orders_customer FOREIGN KEY (customer_id) REFERENCES dbo.customers(customer_id)
);
GO
CREATE INDEX IX_orders_updated_at ON dbo.orders(updated_at);   -- speeds incremental pulls
CREATE INDEX IX_orders_customer   ON dbo.orders(customer_id);
GO

-- ----------------------------------------------------------------------------
-- order_items  (line items inside an order)
-- ----------------------------------------------------------------------------
-- order_items are insert-only (a line item never changes once written), so its
-- watermark is created_at, not updated_at. This keeps the incremental-ingest
-- pattern uniform across orders, order_items, and payments.
CREATE TABLE dbo.order_items (
    order_item_id  INT            IDENTITY(1,1) NOT NULL,
    order_id       INT            NOT NULL,
    product_id     INT            NOT NULL,
    quantity       INT            NOT NULL,
    item_price     DECIMAL(10,2)  NOT NULL,   -- price captured at time of sale
    created_at     DATETIME2(0)   NOT NULL CONSTRAINT DF_items_created DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_order_items PRIMARY KEY (order_item_id),
    CONSTRAINT FK_items_order   FOREIGN KEY (order_id)   REFERENCES dbo.orders(order_id),
    CONSTRAINT FK_items_product FOREIGN KEY (product_id) REFERENCES dbo.products(product_id),
    CONSTRAINT CK_items_qty CHECK (quantity > 0)
);
GO
CREATE INDEX IX_items_order      ON dbo.order_items(order_id);
CREATE INDEX IX_items_created_at ON dbo.order_items(created_at);   -- speeds incremental pulls
GO

-- ----------------------------------------------------------------------------
-- payments  (payment made against an order)
-- ----------------------------------------------------------------------------
CREATE TABLE dbo.payments (
    payment_id  INT            IDENTITY(1,1) NOT NULL,
    order_id    INT            NOT NULL,
    amount      DECIMAL(12,2)  NOT NULL,
    method      NVARCHAR(20)   NOT NULL,   -- UPI / Credit Card / Debit Card / Net Banking / Wallet / COD
    paid_at     DATETIME2(0)   NOT NULL,
    updated_at  DATETIME2(0)   NOT NULL CONSTRAINT DF_payments_updated DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_payments PRIMARY KEY (payment_id),
    CONSTRAINT FK_payments_order FOREIGN KEY (order_id) REFERENCES dbo.orders(order_id)
);
GO
CREATE INDEX IX_payments_updated_at ON dbo.payments(updated_at);
GO

PRINT 'AtliQ Commerce OLTP schema created. Now run 02..06 insert scripts in order.';
GO
