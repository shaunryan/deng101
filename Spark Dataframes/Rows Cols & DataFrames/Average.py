# Databricks notebook source
# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.functions import avg, col, mean
# MAGIC 
# MAGIC df = spark.read.format("json") \
# MAGIC .load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json") \
# MAGIC 
# MAGIC df1 = df \
# MAGIC .select(
# MAGIC   avg("count").alias("avg1"),
# MAGIC   avg(col("count")).alias("avg2"),
# MAGIC   mean("count").alias("avg3"),
# MAGIC   mean(col("count")).alias("avg4"),
# MAGIC   mean(df["count"]).alias("avg5")
# MAGIC )
# MAGIC 
# MAGIC 
# MAGIC display(df1)
