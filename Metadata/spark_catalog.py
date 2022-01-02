# Databricks notebook source
import discover_modules
discover_modules.go(spark)

from utilities import AppConfig


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

spark.catalog.listColumns("billofmaterials")

# COMMAND ----------

dir(spark.catalog)
