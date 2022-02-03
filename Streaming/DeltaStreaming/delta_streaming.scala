// Databricks notebook source
// MAGIC %sql
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
// MAGIC USING delta

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into bronze
// MAGIC values
// MAGIC   (1,'shaun','ryan', now()),
// MAGIC   (2,'john','doe', now()),
// MAGIC   (3,'terry','dilbert', now())

// COMMAND ----------

dbutils.fs.rm("/delta/devtest/_checkpoints/", true)

// COMMAND ----------


val activityCountsQuery = spark.readStream
  .format("delta")
  //.option("ignoreDeletes", "true")
  .table("bronze")
  .writeStream
  .format("delta")
  .option("checkpointLocation", "/delta/devtest/_checkpoints/bronze")
  .outputMode("append")
  .queryName("stream_delta")
  .table("silver")

// COMMAND ----------


for (s <- spark.streams.active)
  println("%s: %s".format(s.name, s.id))


// COMMAND ----------

// MAGIC %sql
// MAGIC select 'bronze' as tableName, * from bronze
// MAGIC union all
// MAGIC select 'silver' as tableName, * from silver
// MAGIC order by Key

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into bronze
// MAGIC values
// MAGIC   (4,'shaun1','ryan', now()),
// MAGIC   (5,'john2','doe', now()),
// MAGIC   (6,'terry3','dilbert', now())

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select 'bronze' as tableName, * from bronze
// MAGIC union all
// MAGIC select 'silver' as tableName, * from silver
// MAGIC order by Key

// COMMAND ----------

for (s <- spark.streams.active) {
  println("Stopping stream: %s".format(s.name))  
  s.stop()
}

// COMMAND ----------

dbutils.fs.rm("/delta/devtest/_checkpoints/bronze", true)
