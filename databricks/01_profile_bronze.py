# Databricks notebook source
from pyspark.sql import functions as F

BRONZE = "abfss://lakehouse@atliqlakeankit.dfs.core.windows.net/bronze"

customers   = spark.read.parquet(f"{BRONZE}/customers")
products    = spark.read.parquet(f"{BRONZE}/products")
orders      = spark.read.parquet(f"{BRONZE}/orders")
order_items = spark.read.parquet(f"{BRONZE}/order_items")
payments    = spark.read.parquet(f"{BRONZE}/payments")

for name, df in [("customers", customers), ("products", products), ("orders", orders),
                 ("order_items", order_items), ("payments", payments)]:
    print(f"{name:14s} {df.count():>8,} rows   {len(df.columns)} cols")

# COMMAND ----------

customers.groupBy("city").count().orderBy("city").show(60, truncate=False)

# COMMAND ----------

products.groupBy("category").count().orderBy("category").show(50, truncate=False)


# COMMAND ----------

orders.groupBy("status").count().orderBy(F.desc("count")).show(50, truncate=False)


# COMMAND ----------

payments.groupBy("method").count().orderBy(F.desc("count")).show(50, truncate=False)

# COMMAND ----------

# QA test accounts
customers.filter(F.col("email").endswith("@atliq-test.com")).count()

# legacy 1900 dates
orders.filter(F.col("order_date") == "1900-01-01").count()

# double-submitted lines
dup_cols = ["order_id", "product_id", "quantity", "item_price", "created_at"]
print(order_items.count(), order_items.dropDuplicates(dup_cols).count())

# retried payments
payments.groupBy("order_id").count().filter("count > 1").count()

# COMMAND ----------

sup = spark.read.parquet(f"{BRONZE}/supplier_price_list")
mkt = spark.read.parquet(f"{BRONZE}/marketing_spend")

sup.show(20, truncate=False)
sup.select("supplier_cost").distinct().show(30, truncate=False)
sup.select("effective_date").distinct().show(30, truncate=False)
mkt.groupBy("channel").count().show(50, truncate=False)

lines = spark.read.text(f"{BRONZE}/clickstream")
print("raw lines:", lines.count())