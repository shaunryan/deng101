// Databricks notebook source
dbutils.fs.rm("adl://gcdatabricksdlsdev.azuredatalakestore.net/databricks/delta/gocompareenergy", true)

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC 
// MAGIC def clearDownDeltaTable(tables:Seq[String])
// MAGIC {
// MAGIC   val delete = tables.map(table => s"delete from ${table}")
// MAGIC   val vacuum = tables.map(table => s"VACUUM ${table} RETAIN 0 HOURS ")
// MAGIC 
// MAGIC   delete.foreach(spark.sql(_))
// MAGIC   spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = false")
// MAGIC   vacuum.foreach(spark.sql(_))
// MAGIC   spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = true")
// MAGIC }
