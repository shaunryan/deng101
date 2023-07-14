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
// MAGIC

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
  .option("ignoreChanges", "true")
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
// MAGIC order by tableName, Key

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC update bronze
// MAGIC set SurnameName = 'mergeme'
// MAGIC where Key = 1
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into bronze
// MAGIC values
// MAGIC   (4,'shaun1','ryan', now()),
// MAGIC   (5,'john2','doe', now()),
// MAGIC   (6,'terry3','dilbert', now())

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger
import io.delta.tables._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

val src = "src"
val dst = "dst"
val uColMap = Map(
          "Forename" -> s"${src}.Forename",
          "SurnameName" -> s"${src}.SurnameName",
          "CreateDate" -> s"${src}.CreateDate")
val iColMap = uColMap + ("Key" -> s"${src}.Key")
val keyMatch = s"${dst}.Key = ${src}.Key"

val activityCountsQuery = spark.readStream
  .format("delta")
//   .option("ignoreDeletes", "true")
  .option("ignoreChanges", "true")
  .table("bronze")
  .writeStream
  .format("delta")
  .trigger(Trigger.Once)
  .option("checkpointLocation", "/delta/devtest/_checkpoints/bronze")
  .outputMode("append")
  .foreachBatch{ (batchDF: DataFrame, batchId: Long) =>
    
    DeltaTable.forName(spark, "silver")
      .as(dst)
      .merge(batchDF.as(src), keyMatch)
      .whenMatched
      .updateExpr(uColMap)
      .whenNotMatched
      .insertExpr(iColMap)
      .execute()
    
    batchDF.show()
  }
  .queryName("stream_delta")
  .start()


// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select 'bronze' as tableName, * from bronze
// MAGIC union all
// MAGIC select 'silver' as tableName, * from silver
// MAGIC order by tableName, Key

// COMMAND ----------

dbutils.notebook.exit("Finished")

// COMMAND ----------

dbutils.fs.rm("/delta/devtest/_checkpoints/bronze", true)

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC drop table if exists bronze;
// MAGIC drop table if exists silver;
// MAGIC drop table if exists gold;
