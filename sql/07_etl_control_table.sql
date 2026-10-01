-- ============================================================================
--  AtliQ Commerce  |  ETL Control Table (metadata-driven ingestion)
--  One row per source object. The Azure Data Factory pipeline reads this table
--  and decides HOW to load each source:
--    * load_type = 'full'         -> copy the whole table every run
--    * load_type = 'incremental'  -> copy only rows where
--                                    <watermark_column> > last_loaded_at
--  After each source loads successfully, the pipeline updates last_loaded_at.
--  This is what makes ONE generic pipeline handle every table.
-- ============================================================================

IF SCHEMA_ID('etl') IS NULL EXEC('CREATE SCHEMA etl;');
GO

IF OBJECT_ID('etl.control_table', 'U') IS NOT NULL DROP TABLE etl.control_table;
GO

CREATE TABLE etl.control_table (
    table_name        NVARCHAR(128) NOT NULL,
    source_schema     NVARCHAR(128) NOT NULL CONSTRAINT DF_ctl_schema DEFAULT 'dbo',
    load_type         NVARCHAR(20)  NOT NULL,     -- 'full' | 'incremental'
    watermark_column  NVARCHAR(128) NULL,         -- NULL for full loads
    last_loaded_at    DATETIME2(0)  NOT NULL CONSTRAINT DF_ctl_last DEFAULT '1900-01-01',
    CONSTRAINT PK_control_table PRIMARY KEY (table_name),
    CONSTRAINT CK_ctl_load_type CHECK (load_type IN ('full', 'incremental'))
);
GO

INSERT INTO etl.control_table (table_name, source_schema, load_type, watermark_column) VALUES
    ('customers',   'dbo', 'full',        NULL),
    ('products',    'dbo', 'full',        NULL),
    ('orders',      'dbo', 'incremental', 'updated_at'),
    ('order_items', 'dbo', 'incremental', 'created_at'),
    ('payments',    'dbo', 'incremental', 'updated_at');
GO

-- Stored proc the pipeline calls after a source loads successfully, to advance
-- its watermark to the timestamp captured at the START of this run.
IF OBJECT_ID('etl.usp_update_watermark', 'P') IS NOT NULL DROP PROCEDURE etl.usp_update_watermark;
GO
CREATE PROCEDURE etl.usp_update_watermark
    @table_name   NVARCHAR(128),
    @run_start_at DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE etl.control_table
       SET last_loaded_at = @run_start_at
     WHERE table_name = @table_name;
END;
GO

PRINT 'ETL control table created and seeded.';
GO
