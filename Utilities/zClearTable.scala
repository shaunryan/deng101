// Databricks notebook source
dbutils.fs.rm(s"${get_storage_account()}/databricks/delta/mydatabase", true)

// COMMAND ----------



def clearDownDeltaTable(tables:Seq[String])
{
  val delete = tables.map(table => s"delete from ${table}")
  val vacuum = tables.map(table => s"VACUUM ${table} RETAIN 0 HOURS ")

  delete.foreach(spark.sql(_))
  spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = false")
  vacuum.foreach(spark.sql(_))
  spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = true")
}
