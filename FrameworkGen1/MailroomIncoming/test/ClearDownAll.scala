// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ## Clear Down

// COMMAND ----------

// MAGIC %scala
// MAGIC val tables = Seq("iautoswitchoptoutrequested",
// MAGIC                  "icancelledswitchresolutionfound",
// MAGIC                  "icoolingoffperiodexpired",
// MAGIC                  "imanualenergyswitchrequested",
// MAGIC                  "isavingsopportunityrejected",
// MAGIC                  "isupplierrequestedswitchcancelled",
// MAGIC                  "iswitchcompleted",
// MAGIC                  "iswitchcompletionunconfirmed",
// MAGIC                  "iswitchdetailssenttosupplier",
// MAGIC                  "iswitchexpired",
// MAGIC                  "iswitchstarted",
// MAGIC                  "iuserrequestedswitchcancelfailed",
// MAGIC                  "iuserrequestedswitchcancellation",
// MAGIC                  "iuserrequestedswitchcancelled",
// MAGIC                  "preferences_iupdated")
// MAGIC 
// MAGIC val deleteStage = tables.map(table => s"delete from mailroomincoming.${table}")
// MAGIC val deleteShred = tables.map(table => s"delete from mailroomincoming.weflip_${table}")
// MAGIC val vacuumStage = tables.map(table => s"VACUUM mailroomincoming.${table} RETAIN 0 HOURS ")
// MAGIC val vacuumShred = tables.map(table => s"VACUUM mailroomincoming.weflip_${table} RETAIN 0 HOURS ")

// COMMAND ----------

deleteStage.foreach(spark.sql(_))
deleteShred.foreach(spark.sql(_))
spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = false")
vacuumStage.foreach(spark.sql(_))
vacuumShred.foreach(spark.sql(_))
spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = true")
