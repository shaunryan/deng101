// Databricks notebook source
import fathom_core.ConnectStorage

ConnectStorage.ConnectStorage()

// COMMAND ----------


import org.apache.spark.sql.types._

val database = "autoloader"
val table = "autoloader"
val checkpointPath = s"${Configuration.getStorageAccount()}checkpoint/raw.${database}.${table}/"

val inputPath = s"${Configuration.getStorageAccount()}raw/${database}/"
val outputPath = s"${Configuration.getStorageAccount()}databricks/delta/${database}/${table}/"
val schema = StructType([StructField('name', StringType(), False)])

val cloudFilesFormat = "json"
val cloudFilesIncludeExistingFiles = "true"
val cloudFilesMaxFilesPerTrigger = "1000"
val cloudFilesMaxBytesPerTrigger = ""
val cloudFilesUseNotifications = "false"
val cloudFilesValidateOptions = "true"
val cloudFilesQueueName = "queuegeneva"


// COMMAND ----------



val df = spark.readStream.format("cloudFiles") 
  .option("cloudFiles.useNotifications"     ,"true") 
  .option("cloudFiles.format"               ,cloudFilesFormat)
  .option("cloudFiles.includeExistingFiles" ,cloudFilesIncludeExistingFiles) 
  .option("cloudFiles.maxFilesPerTrigger"   ,cloudFilesMaxFilesPerTrigger) 
//   .option("cloudFiles.maxBytesPerTrigger"  ,cloudFilesMaxBytesPerTrigger) 
  .option("cloudFiles.useNotifications"     ,cloudFilesUseNotifications) 
  .option("cloudFiles.validateOptions"      ,cloudFilesValidateOptions) 
  .option("cloudFiles.connectionString"     ,Configuration.getDatalakeConnectionString()) 
  .option("cloudFiles.resourceGroup"        ,Configuration.getResourceGroup()) 
  .option("cloudFiles.subscriptionId"       ,Configuration.getSubscriptionId()) 
  .option("cloudFiles.tenantId"             ,Configuration.getAzureADId()) 
  .option("cloudFiles.clientId"             ,Configuration.getServicePrincipalId()) 
  .option("cloudFiles.clientSecret"         ,Configuration.getServiceCredential()) 
  .option("cloudFiles.queueName"            ,cloudFilesQueueName) 
  .schema(schema) 
  .load(inputPath)

df.writeStream.format("delta") 
  .option("checkpointLocation", checkpointPath) 
  .trigger(Trigger.once) 
  .start(outputPath)


// COMMAND ----------


val sqlCreateDb = s"create database if not exists ${database}"

val sqlCreateTable = s"""
  create table if not exists ${database}.${table} (
    name string
  )
  using delta
  location "${outputPath}"
"""

val sqlSelect = s"select * from ${database}.${table}"

spark.sql(sqlCreateDb)
spark.sql(sqlCreateTable)

val df = spark.sql(sqlSelect)

display(df)

// COMMAND ----------

dbutils.notebook.exit("success")

// COMMAND ----------

val sqlCleanup = s"drop table if exists ${database}.${table}"
spark.sql(sqlCleanup)

val sqlCleanup = s"drop database if exists ${database}"
spark.sql(sqlCleanup)

dbutils.fs.rm(s"${Configuration.getStorageAccount()}checkpoint/raw.${database}.${table}", true)
dbutils.fs.rm(s"${Configuration.getStorageAccount()}databricks/delta/${database}", true)
