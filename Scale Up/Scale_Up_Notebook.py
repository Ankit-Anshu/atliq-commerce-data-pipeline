# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # AtliQ Commerce — Scale It Up (optional big-data practice)
# MAGIC Fill in **the next cell only**, then click **Run all**. Everything else is automatic:
# MAGIC
# MAGIC 1. Generates the large dataset (about 1M orders and 10M web events, same kinds of mess as the seed).
# MAGIC 2. Loads it into your Azure SQL tables (replacing the seed rows) and resets the ETL watermarks.
# MAGIC 3. Replaces the CSV and clickstream files in `landing/`.
# MAGIC 4. Clears Bronze, Silver and Gold so your next nightly run rebuilds everything.
# MAGIC 5. Prints the totals your pipeline should reach (also saved as `landing/scale_manifest.json`).
# MAGIC
# MAGIC Then trigger your nightly pipeline. It takes about 30–60 minutes in total.
# MAGIC Read `Scale_Up_Guide.docx` first (cluster, secret scope, cost, and how to go back to the seed).

# COMMAND ----------

# ======== YOUR VALUES (the same storage account and SQL database you used in M1-M7) ========
STORAGE_ACCOUNT = "atliqlakeankit"
SQL_SERVER = "atliq-sql-ankit.database.windows.net"
SQL_DB = "atliq_commerce"
SECRET_SCOPE = "atliq-kv"                                      # must hold the secrets sql-user and sql-password
# ============================================================================================

# COMMAND ----------

import json, os, sys, time

LANDING = f"abfss://lakehouse@{STORAGE_ACCOUNT}.dfs.core.windows.net/landing"
BRONZE = f"abfss://lakehouse@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze"
JDBC_URL = (f"jdbc:sqlserver://{SQL_SERVER}:1433;database={SQL_DB};encrypt=true;"
            "trustServerCertificate=false;loginTimeout=30;")
SQL_USER = dbutils.secrets.get(SECRET_SCOPE, "sql-user")
SQL_PASSWORD = dbutils.secrets.get(SECRET_SCOPE, "sql-password")
TABLES = ["customers", "products", "orders", "order_items", "payments"]


def run_sql(statements):
    """Run T-SQL statements in order, on one connection, from the driver."""
    conn = spark._jvm.java.sql.DriverManager.getConnection(JDBC_URL, SQL_USER, SQL_PASSWORD)
    try:
        stmt = conn.createStatement()
        for s in statements:
            stmt.execute(s)
    finally:
        conn.close()


run_sql(["SELECT 1"])                                     # fail fast if the connection is wrong
print("Connected to Azure SQL.")

# COMMAND ----------

# 1) generate (nothing is changed yet, so a failure here is harmless)
sys.path.append(os.getcwd())                              # atliq_datagen.py sits next to this notebook
import atliq_datagen as gen

t0 = time.time()
key = gen.generate("m", 2026)
frames = gen.oltp_frames_for_load(key)
print(f"Generated in {time.time() - t0:.0f}s:", {n: f"{len(df):,}" for n, df in frames.items()})

# COMMAND ----------

# 2) load into a staging schema, then swap it into the real tables
# ---- promote statements start
PREPARE = ["IF SCHEMA_ID('stage') IS NULL EXEC('CREATE SCHEMA stage')"] + \
          [f"DROP TABLE IF EXISTS stage.{t}" for t in reversed(TABLES)]
PROMOTE = [
    "DELETE FROM dbo.payments", "DELETE FROM dbo.order_items", "DELETE FROM dbo.orders",
    "DELETE FROM dbo.products", "DELETE FROM dbo.customers",
    """SET IDENTITY_INSERT dbo.customers ON;
       INSERT INTO dbo.customers (customer_id, customer_name, email, city, signup_date, updated_at)
       SELECT customer_id, customer_name, email, city, CAST(signup_date AS DATE), CAST(updated_at AS DATETIME2(0))
       FROM stage.customers;
       SET IDENTITY_INSERT dbo.customers OFF;""",
    """SET IDENTITY_INSERT dbo.products ON;
       INSERT INTO dbo.products (product_id, product_name, category, unit_price, updated_at)
       SELECT product_id, product_name, category, CAST(ROUND(unit_price, 2) AS DECIMAL(10,2)),
              CAST(updated_at AS DATETIME2(0))
       FROM stage.products;
       SET IDENTITY_INSERT dbo.products OFF;""",
    """SET IDENTITY_INSERT dbo.orders ON;
       INSERT INTO dbo.orders (order_id, customer_id, order_date, status, order_amount, created_at, updated_at)
       SELECT order_id, customer_id, CAST(order_date AS DATE), status, CAST(ROUND(order_amount, 2) AS DECIMAL(12,2)),
              CAST(created_at AS DATETIME2(0)), CAST(updated_at AS DATETIME2(0))
       FROM stage.orders;
       SET IDENTITY_INSERT dbo.orders OFF;""",
    """SET IDENTITY_INSERT dbo.order_items ON;
       INSERT INTO dbo.order_items (order_item_id, order_id, product_id, quantity, item_price, created_at)
       SELECT order_item_id, order_id, product_id, quantity, CAST(ROUND(item_price, 2) AS DECIMAL(10,2)),
              CAST(created_at AS DATETIME2(0))
       FROM stage.order_items;
       SET IDENTITY_INSERT dbo.order_items OFF;""",
    """SET IDENTITY_INSERT dbo.payments ON;
       INSERT INTO dbo.payments (payment_id, order_id, amount, method, paid_at, updated_at)
       SELECT payment_id, order_id, CAST(ROUND(amount, 2) AS DECIMAL(12,2)), method,
              CAST(paid_at AS DATETIME2(0)), CAST(updated_at AS DATETIME2(0))
       FROM stage.payments;
       SET IDENTITY_INSERT dbo.payments OFF;""",
    "UPDATE etl.control_table SET last_loaded_at = '1900-01-01'",
] + [f"DROP TABLE IF EXISTS stage.{t}" for t in reversed(TABLES)]
# ---- promote statements end

run_sql(PREPARE)
for name in TABLES:
    t = time.time()
    (spark.createDataFrame(frames[name]).repartition(8).write.format("jdbc")
        .option("url", JDBC_URL).option("dbtable", f"stage.{name}")
        .option("user", SQL_USER).option("password", SQL_PASSWORD)
        .option("batchsize", 10000).mode("overwrite").save())
    print(f"stage.{name}: {len(frames[name]):,} rows in {time.time() - t:.0f}s")
t = time.time()
run_sql(PROMOTE)
print(f"Azure SQL now holds the large dataset (swap took {time.time() - t:.0f}s); watermarks reset.")

# COMMAND ----------

# 3) replace the landing files (CSVs + clickstream)
for name, text in gen.csv_texts(key).items():
    dbutils.fs.put(f"{LANDING}/csv/{name}", text, overwrite=True)
dbutils.fs.rm(f"{LANDING}/clickstream", recurse=True)
t, n_files = time.time(), 0
for name, lines in gen.generate_clickstream(key, 2026):
    dbutils.fs.put(f"{LANDING}/clickstream/{name}", "\n".join(lines) + "\n", overwrite=True)
    n_files += 1
print(f"landing/: 2 CSVs + {n_files} clickstream files ({key['raw']['web_lines']:,} lines) in {time.time() - t:.0f}s")

# COMMAND ----------

# 4) clear Bronze, Silver and Gold so the next nightly run rebuilds everything from the large data
#    (if you named your Silver tables differently, add your names to this list)
# 4) clear Bronze, Silver and Gold so the next nightly run rebuilds everything from the large data
for tbl in ["customers", "products", "orders", "order_items", "payments", "supplier_price_list",
            "marketing_spend", "web_events", "web_events_quarantine", "dq_log"]:
    spark.sql(f"DROP TABLE IF EXISTS atliq.silver.{tbl}")

for row in spark.sql("SHOW TABLES IN atliq.gold").collect():
    name = f"atliq.gold.{row.tableName}"
    try:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
    except Exception:
        spark.sql(f"DROP VIEW IF EXISTS {name}")

dbutils.fs.rm(BRONZE, recurse=True)
print("Bronze, Silver and Gold cleared.")

# COMMAND ----------

# 5) the totals your pipeline should reach on this data
manifest = {"dataset": "scale", "seed": 2026, "raw": key["raw"], "clean": key["clean"]}
dbutils.fs.put(f"{LANDING}/scale_manifest.json", json.dumps(manifest, indent=2, default=str), overwrite=True)
print(json.dumps(manifest["clean"], indent=2, default=str))
print("\nDone. Now trigger your nightly pipeline, then compare your totals with the numbers above.")