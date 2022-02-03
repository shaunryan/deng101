// Databricks notebook source

import org.apache.spark.sql.types._

val database = "autoloader"
val table = "autoloader"
val checkpointPath = s"${Configuration.getStorageAccount()}checkpoint/raw.${database}.${table}/"

val inputPath = s"${Configuration.getStorageAccount()}raw/${database}/"
val outputPath = s"${Configuration.getStorageAccount()}databricks/delta/${database}/${table}/"
val schema = StructType([StructField('name', StringType(), False)])

val options = Map(
  "cloudFiles.useNotifications"     -> "true"
  "cloudFiles.format"               -> "json"
  "cloudFiles.includeExistingFiles" -> "true"
  "cloudFiles.maxFilesPerTrigger"   -> "1000"
//   "cloudFiles.maxBytesPerTrigger"   -> ""
  "cloudFiles.useNotifications"     -> "false"
  "cloudFiles.validateOptions"      -> "true"
  "cloudFiles.connectionString"     -> Configuration.getDatalakeConnectionString()
  "cloudFiles.resourceGroup"        -> Configuration.getResourceGroup()
  "cloudFiles.subscriptionId"       -> Configuration.getSubscriptionId()
  "cloudFiles.tenantId"             -> Configuration.getAzureADId()
  "cloudFiles.clientId"             -> Configuration.getServicePrincipalId()
  "cloudFiles.clientSecret"         -> Configuration.getServiceCredential()
  "cloudFiles.queueName"            -> "queuegeneva"
)

// COMMAND ----------



val df = spark.readStream.format("cloudFiles") 
  .options(options) 
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
