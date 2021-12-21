-- Databricks notebook source
-- MAGIC %sql
-- MAGIC 
-- MAGIC select * from mailroomincoming.imanualenergyswitchrequested

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val tables =  spark.catalog.listTables("mailroomincoming")
-- MAGIC display(tables)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val tbls = tables.toDF.collect
-- MAGIC for(t <- tbls)
-- MAGIC {
-- MAGIC 
-- MAGIC   val df = spark.sql(s"select * from ${t.getString(1)}.${t.getString(0)}")
-- MAGIC   println(s"${t.getString(1)}.${t.getString(0)} rows = ${df.count}")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC select * from mailroomincoming.imanualenergyswitchrequested 
