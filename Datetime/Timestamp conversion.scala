// Databricks notebook source
// MAGIC %md
// MAGIC # Convert string date into TimestampType in Spark SQL
// MAGIC This can be done by converting date as string into timestamp (including time zone) using `unix_timestamp` and casting it as `TimestampType`, see example below. Note that you might need to convert with some specific timezone.

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

val df = Seq(
  (1, "2014/01/01 23:00:01"), 
  (1, "2014/11/31 12:40:32"), 
  (1, "2016/12/29 09:54:00"), 
  (1, "2016/05/09 10:12:43")).toDF("id", "date")

// COMMAND ----------

val res = df.select($"id", $"date", unix_timestamp($"date", "yyyy/MM/dd HH:mm:ss").cast(TimestampType).as("timestamp"), current_timestamp(), current_date())

// COMMAND ----------

res.printSchema

// COMMAND ----------

res.show(false)
