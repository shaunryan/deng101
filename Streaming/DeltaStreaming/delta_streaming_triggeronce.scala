// Databricks notebook source
dbutils.fs.rm("/delta/devtest/_checkpoints/bronze", true)

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

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into bronze
// MAGIC values
// MAGIC   (1,'shaun','ryan', now()),
// MAGIC   (2,'john','doe', now()),
// MAGIC   (3,'terry','dilbert', now())

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger

val activityCountsQuery = spark.readStream
  .format("delta")
//   .option("ignoreDeletes", "true")
//   .option("ignoreChanges", "true")
  .table("bronze")
  .writeStream
  .format("delta")
  .trigger(Trigger.Once)
  .option("checkpointLocation", "/delta/devtest/_checkpoints/bronze")
  .outputMode("append")
  .queryName("stream_delta")
  .table("silver")

// COMMAND ----------

for (s <- spark.streams.active)
  println("%s: %s".format(s.name, s.id))

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select 'bronze' as tableName, * from bronze
// MAGIC union all
// MAGIC select 'silver' as tableName, * from silver
// MAGIC order by tableName, Key

// COMMAND ----------

// MAGIC %sql
// MAGIC -- delete from bronze
// MAGIC -- where Key = 1
// MAGIC 
// MAGIC update bronze
// MAGIC set SurnameName = 'wow'

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC   
// MAGIC insert into bronze
// MAGIC SELECT 4 as Key,'shaun1' as Forename,'ryan' as SurnameName, now() as CreateDate
// MAGIC UNION ALL
// MAGIC SELECT 5 as Key,'john2' as Forename,'doe' as SurnameName, now() as CreateDate
// MAGIC UNION ALL
// MAGIC SELECT 6 as Key,'shaun1' as Forename,'ryan' as SurnameName, to_timestamp('2021-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') as CreateDate

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger

val activityCountsQuery = spark.readStream
  .format("delta")
//   .option("ignoreDeletes", "true")
//   .option("ignoreChanges", "true")
  .table("bronze")
  .writeStream
  .format("delta")
  .trigger(Trigger.Once)
  .option("checkpointLocation", "/delta/devtest/_checkpoints/bronze")
  .outputMode("append")
  .queryName("stream_delta")
  .table("silver")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select 'bronze' as tableName, * from bronze
// MAGIC union all
// MAGIC select 'silver' as tableName, * from silver
// MAGIC order by tableName, key

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
