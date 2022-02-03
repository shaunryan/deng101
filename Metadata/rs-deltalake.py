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

# MAGIC %pip install deltalake

# COMMAND ----------

display(dbutils.fs.ls("/delta/jaffle_shop/None/customers"))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe extended jaffle_shop.customers

# COMMAND ----------



from deltalake import DeltaTable
path = "/delta/jaffle_shop/None/"
dt = DeltaTable(path)
data = dt.to_pyarrow_dataset().to_table().to_pydict()
