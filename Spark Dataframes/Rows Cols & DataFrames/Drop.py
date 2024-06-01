# Databricks notebook source

df = spark.read.format("json") \
.load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json")

display(df)

# COMMAND ----------

display(df.describe())

# COMMAND ----------

df1 = df.drop("DEST_COUNTRY_NAME", "count")
display(df1.distinct())

# COMMAND ----------

dcols = ["DEST_COUNTRY_NAME", "count"]
df2 = df.drop(*dcols)
display(df2.distinct())

# COMMAND ----------


df3 = df.drop(df["DEST_COUNTRY_NAME"]).drop(df["count"])
display(df3.distinct())
