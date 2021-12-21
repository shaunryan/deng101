-- Databricks notebook source
-- MAGIC %scala
-- MAGIC assert(spark.sessionState.conf.bucketingEnabled, "Bucketing disabled?!")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", false)
-- MAGIC spark.conf.getOption("spark.databricks.delta.retentionDurationCheck.enabled")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC //spark.sql("SET spark.sql.cbo.starSchemaDetection=true").show(truncate = true)
-- MAGIC spark.conf.set("spark.sql.cbo.enabled", true)
-- MAGIC spark.conf.set("spark.sql.cbo.starSchemaDetection", true)
-- MAGIC 
-- MAGIC 
-- MAGIC spark.conf.getOption("spark.sql.cbo.starSchemaDetection")
-- MAGIC spark.conf.getOption("spark.sql.cbo.enabled")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.conf.getOption("spark.network.crypto.enabled")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.conf.set("spark.network.crypto.enabled", true)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.conf.getOption("spark.sql.execution.arrow.enabled")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.conf.getOption("spark.sql.caseSensitive")
