# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS atliq
# MAGIC   MANAGED LOCATION 'abfss://lakehouse@atliqlakeankit.dfs.core.windows.net/atliq-managed';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS atliq.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS atliq.gold;

# COMMAND ----------

BRONZE = "abfss://lakehouse@atliqlakeankit.dfs.core.windows.net/bronze"
display(spark.read.parquet(f"{BRONZE}/customers"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN atliq;