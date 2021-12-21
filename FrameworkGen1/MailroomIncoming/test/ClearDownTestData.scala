// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ## Clear Down Test Data

// COMMAND ----------

// MAGIC %run AzureDataFactory/Includes/SetupEnvironment

// COMMAND ----------

ConnectToDataLake()

// COMMAND ----------

// MAGIC %scala
// MAGIC val root = getDataLakeStorageAccount
// MAGIC 
// MAGIC //DO NOT CHANGE THESE PATHS WITHOUT CODE REVIEW BEFORE EXECUTE
// MAGIC dbutils.fs.rm(s"${root}/raw/mailroom/data/schemaversion0/1900/01", true)

// COMMAND ----------

val tables = Seq("iautoswitchoptoutrequested",
                 "icancelledswitchresolutionfound",
                 "icoolingoffperiodexpired",
                 "imanualenergyswitchrequested",
                 "isavingsopportunityrejected",
                 "isupplierrequestedswitchcancelled",
                 "iswitchcompleted",
                 "iswitchcompletionunconfirmed",
                 "iswitchdetailssenttosupplier",
                 "iswitchexpired",
                 "iswitchstarted",
                 "iuserrequestedswitchcancelfailed",
                 "iuserrequestedswitchcancellation",
                 "iuserrequestedswitchcancelled",
                 "preferences_iupdated")

val deleteStage = tables.map(table => s"delete from mailroomincoming.${table}")
val deleteShred = tables.map(table => s"delete from mailroomincoming.weflip_${table}")
val vacuumStage = tables.map(table => s"VACUUM mailroomincoming.${table} RETAIN 0 HOURS ")
val vacuumShred = tables.map(table => s"VACUUM mailroomincoming.weflip_${table} RETAIN 0 HOURS ")


// COMMAND ----------

deleteStage.foreach(spark.sql(_))
deleteShred.foreach(spark.sql(_))
spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = false")
vacuumStage.foreach(spark.sql(_))
vacuumShred.foreach(spark.sql(_))
spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = true")

// COMMAND ----------

// MAGIC %scala
// MAGIC dbutils.notebook.exit(s"success")
