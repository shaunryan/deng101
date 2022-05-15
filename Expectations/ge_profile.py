# Databricks notebook source
# MAGIC %pip install great-expectations==0.14.12

# COMMAND ----------

# dbutils.widgets.removeAll()
dbutils.widgets.text("Database", "jaffle_shop", "Database")
dbutils.widgets.text("Table", "orders", "Table")

# COMMAND ----------

db = dbutils.widgets.get("Database")
table = dbutils.widgets.get("Table")

# COMMAND ----------

import great_expectations as ge
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.render.renderer import *
from great_expectations.render.view import DefaultJinjaPageView

# COMMAND ----------

df = spark.sql(f"select * from {db}.{table}")

# COMMAND ----------

expectation_suite, validation_result = BasicDatasetProfiler.profile(SparkDFDataset(df))
document_model = ProfilingResultsPageRenderer().render(validation_result)
displayHTML(DefaultJinjaPageView().render(document_model))
