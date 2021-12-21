# Databricks notebook source
from fathom.ConnectStorage import ConnectStorage

ConnectStorage()

# COMMAND ----------

from pyspark.sql.types import *
from fathom.Configuration import *

database = "autoloader"
table = "autoloader"
checkpointPath = f"{getStorageAccount()}checkpoint/raw.{database}.{table}/"

inputPath = f"{getStorageAccount()}raw/autoloader/"
outputPath = f"{getStorageAccount()}databricks/delta/{database}/{table}/"
schema = StructType([StructField('name', StringType(), False)])

cloudFilesFormat = "json"
cloudFilesIncludeExistingFiles = "true"
cloudFilesMaxFilesPerTrigger = "1000"
cloudFilesMaxBytesPerTrigger = ""
cloudFilesUseNotifications = "false"
cloudFilesValidateOptions = "true"
cloudFilesQueueName = "queuegeneva"


# COMMAND ----------

  #.option("cloudFiles.maxBytesPerTrigger"  ,cloudFilesMaxBytesPerTrigger) \
df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.useNotifications"     ,"true") \
  .option("cloudFiles.format"               ,cloudFilesFormat) \
  .option("cloudFiles.includeExistingFiles" ,cloudFilesIncludeExistingFiles) \
  .option("cloudFiles.maxFilesPerTrigger"   ,cloudFilesMaxFilesPerTrigger) \
  .option("cloudFiles.useNotifications"     ,cloudFilesUseNotifications) \
  .option("cloudFiles.validateOptions"      ,cloudFilesValidateOptions) \
  .option("cloudFiles.connectionString"     ,getDatalakeConnectionString()) \
  .option("cloudFiles.resourceGroup"        ,getResourceGroup()) \
  .option("cloudFiles.subscriptionId"       ,getSubscriptionId()) \
  .option("cloudFiles.tenantId"             ,getAzureADId()) \
  .option("cloudFiles.clientId"             ,getServicePrincipalId()) \
  .option("cloudFiles.clientSecret"         ,getServiceCredential()) \
  .option("cloudFiles.queueName"            ,cloudFilesQueueName) \
  .schema(schema) \
  .load(inputPath)

df.writeStream.format("delta") \
  .option("checkpointLocation", checkpointPath) \
  .trigger(once=True) \
  .start(outputPath)


# COMMAND ----------


sqlCreateDb = f"create database if not exists {database}"

sqlCreateTable = f"""
  create table if not exists {database}.{table} (
    name string
  )
  using delta
  location "{outputPath}"
"""

sqlSelect = f"select * from {database}.{table}"

spark.sql(sqlCreateDb)
spark.sql(sqlCreateTable)
df = spark.sql(sqlSelect)

display(df)

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------


sqlCleanup = f"drop table if exists {database}.{table}"
spark.sql(sqlCleanup)

sqlCleanup = f"drop database if exists {database}"
spark.sql(sqlCleanup)

dbutils.fs.rm(f"{getStorageAccount()}checkpoint/raw.{database}.{table}", True)
dbutils.fs.rm(f"{getStorageAccount()}databricks/delta/{database}", True)
