# Databricks notebook source
from pyspark.sql.functions import expr, col, column
from pyspark.sql.types import DoubleType

df = spark.read.format("json") \
.load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json") 

df = (df
      .withColumn("count_decimal", expr("cast(count as DOUBLE)"))
      .withColumn("count_decimal2", df["count"].cast(DoubleType()))
      .withColumn("count_decimal3", df["count"].cast("Double"))
      .withColumn("count_decimal3", col("count").cast("Double"))
     )
display(df)
