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
# MAGIC display(df)

# COMMAND ----------

from pyspark.sql.functions import col

df.where("ORIGIN_COUNTRY_NAME like 'R%'").show(2)
df.filter(col("ORIGIN_COUNTRY_NAME").contains("R")).show(2)
df.filter(col("ORIGIN_COUNTRY_NAME").like("R%")).show(2)

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(col("count") < 2).show(2)
df.where("count < 2").show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In Spark 3.0, you can use `joinedDF.explain('mode')` to display a readable and digestible output. 
# MAGIC 
# MAGIC The modes include:
# MAGIC 
# MAGIC - simple
# MAGIC - extended
# MAGIC - codegen
# MAGIC - cost
# MAGIC - formatted
