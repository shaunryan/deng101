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

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## I have Great Expectations!

# COMMAND ----------

ge_path = '/great_expectations'
dbutils.fs.mkdirs(ge_path)
display(dbutils.fs.ls('/'))

# COMMAND ----------

import great_expectations as ge
from great_expectations.datasource.generator.databricks_generator import DatabricksTableBatchKwargsGenerator
from great_expectations.datasource.sparkdf_datasource import SparkDFDatasource
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
import json

print(f'/dbfs{ge_path}')
context = ge.data_context.DataContext(f'/dbfs{ge_path}')

def build_expectations(database, table_name, context):
  
  time_stamp = str(datetime.today().strftime("%d-%m-%Y"))
  exp_suite = f'{database}.{table_name}.{time_stamp}+{_expectations}'
  data = spark.table(database + "." + table_name)
  spark_data = SparkDFDataset(data)
  context.create_expectation_suite(exp_suite, overwrite_existing = True)
  profile = spark_data.profile(BasicDatasetProfiler)
  context.save_expectation_suite(profile)

build_expectations(database, table_name, context)


# COMMAND ----------

gdf.head()
