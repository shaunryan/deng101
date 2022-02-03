# Databricks notebook source
import logging, os
from logger import get_logger

notebook = os.path.basename(os.getcwd())
logger = get_logger(notebook, logging.INFO)

# COMMAND ----------

import discover_modules
discover_modules.go(spark)
from utilities import AppConfig

app_config = AppConfig(dbutils, spark)
app_config.connect_storage()
display(dbutils.fs.ls(app_config.get_storage_account()))


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC use AdventureWorks;
# MAGIC show tables;

# COMMAND ----------

display(spark.catalog.listDatabases())


# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

spark.catalog.listTables("AdventureWorks")

# COMMAND ----------

spark.catalog.listColumns(table)

# COMMAND ----------

from pprint import pprint
for t in spark.catalog.listTables("AdventureWorks"):
  logger.info(f"{t.database}.{t.name}")
  pprint(spark.catalog.listColumns(t.name, t.database))
