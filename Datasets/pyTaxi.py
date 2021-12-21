# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Mount Up the Data

# COMMAND ----------

table_name = 'nyctaxi_yellow'
database = 'nyctaxi'
path = f'dbfs:/databricks-datasets/{database}/tables/{table_name}/'

# COMMAND ----------


spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"CREATE TABLE IF NOT EXISTS {database}.{table_name} USING DELTA LOCATION '{path}'")

# COMMAND ----------

df = spark.table(f'{database}.{table_name}')
display(df)
