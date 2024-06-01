# Databricks notebook source

from pyspark.sql.functions import avg, col, mean

df = spark.read.format("json") \
.load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json") \

df1 = df \
.select(
  avg("count").alias("avg1"),
  avg(col("count")).alias("avg2"),
  mean("count").alias("avg3"),
  mean(col("count")).alias("avg4"),
  mean(df["count"]).alias("avg5")
)


display(df1)
