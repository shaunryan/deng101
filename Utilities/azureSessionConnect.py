# Databricks notebook source
import discover_modules
discover_modules.go(spark)

# COMMAND ----------

from utilities import AppConfig

app_config = AppConfig(dbutils, spark)
app_config.help()


# COMMAND ----------

app_config.connect_storage()
display(dbutils.fs.ls(app_config.get_storage_account()))
