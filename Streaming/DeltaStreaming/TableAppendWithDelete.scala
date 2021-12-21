// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ## Create the Demo Tables

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC drop table if exists bronze;
// MAGIC create table bronze
// MAGIC (
// MAGIC   Key int,
// MAGIC   Forename string,
// MAGIC   SurnameName string,
// MAGIC   CreateDate Timestamp
// MAGIC )
// MAGIC USING delta;
// MAGIC 
// MAGIC drop table if exists silver;
// MAGIC create table silver
// MAGIC (
// MAGIC   Key int,
// MAGIC   Forename string,
// MAGIC   SurnameName string,
// MAGIC   CreateDate Timestamp
// MAGIC )
// MAGIC USING delta;
// MAGIC 
// MAGIC drop table if exists gold;
// MAGIC create table gold
// MAGIC (
// MAGIC   Key int,
// MAGIC   Forename string,
// MAGIC   SurnameName string,
// MAGIC   CreateDate Timestamp
// MAGIC )
// MAGIC USING delta;
// MAGIC 
// MAGIC ALTER TABLE bronze
// MAGIC SET TBLPROPERTIES (
// MAGIC   delta.logRetentionDuration = "interval 1 days",
// MAGIC   delta.deletedFileRetentionDuration = "interval 1 days"
// MAGIC )

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Insert Some Data to Start With

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into bronze
// MAGIC values
// MAGIC   (1,'shaun','ryan', now()),
// MAGIC   (2,'john','doe', now()),
// MAGIC   (3,'terry','dilbert', now())

// COMMAND ----------

// MAGIC %md
// MAGIC ## Start the Stream

// COMMAND ----------


val activityCountsQuery = spark.readStream
  .format("delta")
  .option("ignoreDeletes", "false")
  .table("bronze")
  .writeStream
  .format("delta")
  .option("checkpointLocation", "/delta/devtest/_checkpoints/bronze")
  .outputMode("append")
  .queryName("stream_delta")
  .table("silver")


// COMMAND ----------

// MAGIC %md
// MAGIC ## Check the Stream

// COMMAND ----------

for (s <- spark.streams.active)
  println("%s: %s".format(s.name, s.id))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Check the Data Has Streamed

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select 'bronze' as tableName, * from bronze
// MAGIC union all
// MAGIC select 'silver' as tableName, * from silver
// MAGIC order by Key, tableName

// COMMAND ----------

// MAGIC %md
// MAGIC ## Try Deleting Some Data

// COMMAND ----------

// MAGIC %sql
// MAGIC delete from bronze
// MAGIC where Key = 1

// COMMAND ----------

spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = false")
spark.sql("VACUUM bronze RETAIN 0 HOURS")
spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = true")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select 'bronze' as tableName, * from bronze
// MAGIC union all
// MAGIC select 'silver' as tableName, * from silver
// MAGIC order by Key, tableName

// COMMAND ----------

// MAGIC %md
// MAGIC ## Stop the Stream & Clean-up

// COMMAND ----------

for (s <- spark.streams.active) {
  println("Stopping stream: %s".format(s.name))  
  s.stop()
}

// COMMAND ----------

dbutils.fs.rm("/delta/devtest/_checkpoints/bronze", true)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC drop table if exists bronze;
// MAGIC drop table if exists silver;
// MAGIC drop table if exists gold;
