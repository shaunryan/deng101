# Databricks notebook source
display(dbutils.fs.ls("/databricks-datasets/"))

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/cs100/lab1/"))

# COMMAND ----------

with open("/dbfs/databricks-datasets/airlines/README.md") as f:
    x = ''.join(f.readlines())

print(x)
