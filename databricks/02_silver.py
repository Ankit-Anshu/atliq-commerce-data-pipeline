# Databricks notebook source
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

BRONZE = "abfss://lakehouse@atliqlakeankit.dfs.core.windows.net/bronze"

dbutils.widgets.text("run_date", "")          
run_date = dbutils.widgets.get("run_date")   
print("run_date =", run_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## cleaning helpers

# COMMAND ----------

def norm(col):
    """trimmed, lower-case, single-spaced text: the key every lookup map uses"""
    return F.lower(F.regexp_replace(F.trim(col), r"\s+", " "))

STATUS_MAP = {"placed": "Placed", "shipped": "Shipped", "delivered": "Delivered",
              "cancelled": "Cancelled", "canceled": "Cancelled", "returned": "Returned"}
status_map = F.create_map(*[F.lit(x) for kv in STATUS_MAP.items() for x in kv])

CITY_MAP = {"bengaluru": "Bengaluru", "bangalore": "Bengaluru", "mumbai": "Mumbai",
            "delhi": "Delhi", "hyderabad": "Hyderabad", "chennai": "Chennai",
            "pune": "Pune", "kolkata": "Kolkata", "ahmedabad": "Ahmedabad",
            "jaipur": "Jaipur", "surat": "Surat"}
city_map = F.create_map(*[F.lit(x) for kv in CITY_MAP.items() for x in kv])

CATEGORY_MAP = {"electronics": "Electronics", "home & kitchen": "Home & Kitchen",
                "home and kitchen": "Home & Kitchen", "fashion": "Fashion",
                "books": "Books", "beauty": "Beauty", "sports": "Sports"}
category_map = F.create_map(*[F.lit(x) for kv in CATEGORY_MAP.items() for x in kv])

METHOD_MAP = {"upi": "UPI", "credit card": "Credit Card", "debit card": "Debit Card",
              "net banking": "Net Banking", "wallet": "Wallet", "cod": "COD"}
method_map = F.create_map(*[F.lit(x) for kv in METHOD_MAP.items() for x in kv])

CHANNEL_MAP = {"meta ads": "Meta Ads", "google ads": "Google Ads", "email": "Email",
               "seo": "SEO", "influencer": "Influencer"}
channel_map = F.create_map(*[F.lit(x) for kv in CHANNEL_MAP.items() for x in kv])

def log_dq(table_name, rule, rows_affected):
    (spark.createDataFrame([(run_date, table_name, rule, int(rows_affected))],
        "run_date STRING, table_name STRING, rule STRING, rows_affected LONG")
        .write.mode("append").saveAsTable("atliq.silver.dq_log"))

# COMMAND ----------

customers = (spark.read.parquet(f"{BRONZE}/customers")
    .withColumn("city", city_map[norm(F.col("city"))])
    .withColumn("email", F.lower(F.trim("email")))
    .withColumn("is_test", F.col("email").endswith("@atliq-test.com"))
    .withColumn("signup_date", F.to_date("signup_date"))
    .dropDuplicates(["customer_id"])
    .filter(F.col("customer_id").isNotNull()))

customers.write.format("delta").mode("overwrite").saveAsTable("atliq.silver.customers")

log_dq("customers", "test accounts flagged", customers.filter("is_test").count())

# COMMAND ----------

products = (spark.read.parquet(f"{BRONZE}/products")
    .withColumn("category", category_map[norm(F.col("category"))])
    .withColumn("product_name", F.regexp_replace(F.trim("product_name"), r"\s+", " "))
    .dropDuplicates(["product_id"])
    .filter(F.col("product_id").isNotNull()))

products.write.format("delta").mode("overwrite").saveAsTable("atliq.silver.products")

# COMMAND ----------

sup = spark.read.parquet(f"{BRONZE}/supplier_price_list")

amount = F.regexp_replace(
    F.regexp_extract("supplier_cost", r"-?[0-9][0-9,]*(\.[0-9]+)?", 0), ",", "")

sup = (sup.withColumn("supplier_cost", F.when(amount != "", amount.cast("decimal(12,2)")))
          .withColumn("effective_date", F.coalesce(
              *[F.to_date(F.try_to_timestamp("effective_date", F.lit(f)))
                for f in ["yyyy-MM-dd", "dd/MM/yyyy", "dd-MMM-yyyy"]])))

before = sup.count()
sup = sup.dropDuplicates()
log_dq("supplier_price_list", "exact duplicate rows removed", before - sup.count())

known = spark.table("atliq.silver.products").select("product_id")
before = sup.count()
sup = sup.join(known, "product_id", "inner")
log_dq("supplier_price_list", "rows for unknown products dropped", before - sup.count())

sup.write.format("delta").mode("overwrite").saveAsTable("atliq.silver.supplier_price_list")

# COMMAND ----------

mkt = spark.read.parquet(f"{BRONZE}/marketing_spend")

spend  = F.regexp_replace(F.regexp_extract("spend_amount", r"-?[0-9][0-9,]*(\.[0-9]+)?", 0), ",", "")
clicks = F.regexp_replace(F.regexp_extract("clicks", r"-?[0-9][0-9,]*", 0), ",", "")

mkt = (mkt.withColumn("channel", channel_map[norm(F.col("channel"))])
          .withColumn("spend_amount", F.when(spend  != "", spend.cast("decimal(12,2)")))
          .withColumn("clicks",       F.when(clicks != "", clicks.cast("bigint")))
          .withColumn("spend_date",   F.to_date("spend_date")))

before = mkt.count()
mkt = mkt.dropDuplicates()
log_dq("marketing_spend", "exact duplicate rows removed", before - mkt.count())

before = mkt.count()
mkt = mkt.filter(F.col("spend_amount").isNotNull())
log_dq("marketing_spend", "blank-spend rows dropped", before - mkt.count())

mkt.write.format("delta").mode("overwrite").saveAsTable("atliq.silver.marketing_spend")

# COMMAND ----------

def batch_exists(path: str) -> bool:
    """True when this run's Bronze folder exists and holds at least one Parquet file."""
    try:
        return any(f.name.endswith(".parquet") for f in dbutils.fs.ls(path))
    except Exception as e:
        if "FileNotFound" in str(e) or "PATH_NOT_FOUND" in str(e):
            return False
        raise   # anything else (e.g. permissions) should still fail the run

src_path = f"{BRONZE}/orders/ingest_date={run_date}"

if not batch_exists(src_path):
    print(f"No new orders batch for {run_date}: Silver unchanged.")
else:
    batch = spark.read.parquet(src_path)

    # keep the latest version of each order in this batch
    w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    src = (batch.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
        .withColumn("status", status_map[norm(F.col("status"))])
        .withColumn("order_date",
            F.when(F.col("order_date") == F.lit("1900-01-01").cast("date"),
                   F.to_date("created_at")).otherwise(F.col("order_date")))
        .withColumn("order_amount", F.col("order_amount").cast("decimal(12,2)"))
        .filter(F.col("order_id").isNotNull()))

    # make sure the target exists (first run), then upsert on the business key
    (DeltaTable.createIfNotExists(spark)
        .tableName("atliq.silver.orders").addColumns(src.schema).execute())

    (DeltaTable.forName(spark, "atliq.silver.orders").alias("t")
        .merge(src.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

src_path = f"{BRONZE}/payments/ingest_date={run_date}"

if not batch_exists(src_path):
    print(f"No new payments batch for {run_date}: Silver unchanged.")
else:
    batch = spark.read.parquet(src_path)

    w = Window.partitionBy("payment_id").orderBy(F.col("updated_at").desc())
    src = (batch.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
        .withColumn("method", method_map[norm(F.col("method"))])
        .withColumn("amount", F.col("amount").cast("decimal(12,2)"))
        .filter(F.col("payment_id").isNotNull()))

    (DeltaTable.createIfNotExists(spark)
        .tableName("atliq.silver.payments").addColumns(src.schema).execute())

    (DeltaTable.forName(spark, "atliq.silver.payments").alias("t")
        .merge(src.alias("s"), "t.payment_id = s.payment_id")
        .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

src_path = f"{BRONZE}/order_items/ingest_date={run_date}"

if not batch_exists(src_path):
    print(f"No new order_items batch for {run_date}: Silver unchanged.")
else:
    src = (spark.read.parquet(src_path)
        .withColumn("item_price", F.col("item_price").cast("decimal(12,2)"))
        .filter(F.col("order_item_id").isNotNull()))

    (DeltaTable.createIfNotExists(spark)
        .tableName("atliq.silver.order_items").addColumns(src.schema).execute())

    (DeltaTable.forName(spark, "atliq.silver.order_items").alias("t")
        .merge(src.alias("s"), "t.order_item_id = s.order_item_id")
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

SCHEMA = ("event_id STRING, session_id STRING, customer_id BIGINT, event_type STRING, "
          "event_ts STRING, product_id BIGINT, order_id BIGINT, device STRING, "
          "user_agent STRING, utm_source STRING")

lines = spark.read.text(f"{BRONZE}/clickstream")        # raw lines, as ADF copied them
parsed = lines.withColumn("e", F.from_json("value", SCHEMA))

(parsed.filter("e.event_id IS NULL").select("value")
    .write.mode("overwrite").saveAsTable("atliq.silver.web_events_quarantine"))

ts = (F.when(F.col("event_ts").rlike(r"^\d+$"),
             F.timestamp_millis(F.col("event_ts").cast("bigint")))
        .otherwise(F.try_to_timestamp("event_ts", F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSX"))))

ev = (parsed.filter("e.event_id IS NOT NULL").select("e.*")
    .withColumn("event_ts", ts)
    .withColumn("event_type", norm(F.col("event_type")))
    .dropDuplicates(["event_id"])
    .filter(F.col("session_id").isNotNull() & (F.trim("session_id") != "")))

bots = (ev.filter(F.col("user_agent").rlike(r"(?i)(bot|crawler|spider|python-requests)"))
          .select("session_id").distinct())
known = spark.table("atliq.silver.products").select(F.col("product_id").alias("_pid"))

ev = (ev.join(bots, "session_id", "left_anti")
        .join(known, F.col("product_id") == F.col("_pid"), "left")
        .filter(F.col("product_id").isNull() | F.col("_pid").isNotNull()).drop("_pid"))

ev.write.mode("overwrite").saveAsTable("atliq.silver.web_events")