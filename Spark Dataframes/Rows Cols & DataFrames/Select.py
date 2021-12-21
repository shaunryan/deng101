# Databricks notebook source
# MAGIC %scala
# MAGIC 
# MAGIC val df = spark.read.format("json")
# MAGIC .load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC df = spark.read.format("json") \
# MAGIC .load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.functions.{expr, col, column}
# MAGIC 
# MAGIC df.select(
# MAGIC   df.col("DEST_COUNTRY_NAME"),
# MAGIC   col("DEST_COUNTRY_NAME"),
# MAGIC   column("DEST_COUNTRY_NAME"),
# MAGIC   'DEST_COUNTRY_NAME,
# MAGIC   $"DEST_COUNTRY_NAME",
# MAGIC   expr("DEST_COUNTRY_NAME")
# MAGIC ).show(2)

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import expr, col, column
# MAGIC 
# MAGIC df.select(
# MAGIC   expr("DEST_COUNTRY_NAME"),
# MAGIC   col("DEST_COUNTRY_NAME"),
# MAGIC   df["DEST_COUNTRY_NAME"],
# MAGIC   df.DEST_COUNTRY_NAME,
# MAGIC   "DEST_COUNTRY_NAME"
# MAGIC ).show(2)

# COMMAND ----------

# MAGIC %scala
# MAGIC // and python
# MAGIC 
# MAGIC df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

# COMMAND ----------

df.select(
  expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME")
).show(2)

df.selectExpr(
  "DEST_COUNTRY_NAME as destination", 
  "DEST_COUNTRY_NAME"
).show(2)

df.selectExpr(
  "*",
  "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry"
).show(2)

df.selectExpr(
  "avg(count)", 
  "count(distinct(DEST_COUNTRY_NAME))"
).show(2)

