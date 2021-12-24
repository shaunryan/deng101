# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Schema Merging
# MAGIC 
# MAGIC Column Mapping for Renames - https://docs.databricks.com/delta/delta-column-mapping.html

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean Up

# COMMAND ----------

dbutils.fs.rm("/delta/mergeschema", True)


# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists default.TestSchemaMerge;
# MAGIC Create table default.TestSchemaMerge (
# MAGIC some STRING,
# MAGIC col STRING,
# MAGIC names LONG
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/delta/mergeschema/'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5',
# MAGIC   'delta.columnMapping.mode' = 'name'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Lay Down a Basic Table

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql import Row
# MAGIC from pyspark.sql.types import StructField, StructType, StringType, LongType
# MAGIC 
# MAGIC schema = StructType([
# MAGIC   StructField("some", StringType(), True),
# MAGIC   StructField("col", StringType(), True),
# MAGIC   StructField("names", LongType(), False)
# MAGIC ])
# MAGIC 
# MAGIC rows = [Row("One", "A", 1), 
# MAGIC           Row("Two", "B", 2), 
# MAGIC           Row("Three", "C", 3)]
# MAGIC 
# MAGIC src_df = spark.createDataFrame(rows, schema)
# MAGIC 
# MAGIC (src_df.write
# MAGIC   .format("delta")
# MAGIC   .option("mergeSchema", "true")
# MAGIC   .mode("append")
# MAGIC   .saveAsTable("default.TestSchemaMerge"))
# MAGIC #   .save("/delta/mergeschema/"))
# MAGIC 
# MAGIC chk_df = spark.read.format("delta").load("/delta/mergeschema/")
# MAGIC display(chk_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create A Change Schema
# MAGIC 
# MAGIC - New ohno column added in the middle
# MAGIC - Old col removed

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql import Row
# MAGIC from pyspark.sql.types import StructField, StructType, StringType, LongType
# MAGIC 
# MAGIC schema = StructType([
# MAGIC   StructField("some", StringType(), True),
# MAGIC #   StructField("col", StringType(), True),
# MAGIC   StructField("ohno", StringType(), True),
# MAGIC   StructField("names", LongType(), False)
# MAGIC ])
# MAGIC 
# MAGIC rows = [Row("One", "tada", 1), 
# MAGIC         Row("Two", "check", 2), 
# MAGIC         Row("Three", "me", 3)]
# MAGIC 
# MAGIC src_df = spark.createDataFrame(rows, schema)
# MAGIC display(src_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Write the Change Schema

# COMMAND ----------

(src_df.write
  .format("delta")
  .option("mergeSchema", "true")
  .mode("append")
  .save("/delta/mergeschema/"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Check the table
# MAGIC 
# MAGIC A-ok! New columns and deleted columns are handled fine, reading by files and sql.

# COMMAND ----------

chk_df = spark.read.format("delta").load("/delta/mergeschema/")
display(chk_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM default.TestSchemaMerge

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Alter a Table Column
# MAGIC 
# MAGIC Without re-writing all the data.
# MAGIC 
# MAGIC - change ohno to newcol

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC ALTER TABLE default.TestSchemaMerge RENAME COLUMN ohno TO newcol;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.TestSchemaMerge;

# COMMAND ----------

chk_df = spark.read.format("delta").load("/delta/mergeschema/")
display(chk_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Rename Using Create or Replace
# MAGIC 
# MAGIC - CREATE OR REPLACE - succeeds but breaks the table. Even though the data is there it cannot be retrieved
# MAGIC - DROP AND CREATE - the drop succeeds but the create fails because the column names are different

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Create or replace table default.TestSchemaMerge (
# MAGIC some STRING,
# MAGIC col STRING,
# MAGIC names LONG,
# MAGIC evenewercol STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/delta/mergeschema/'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5',
# MAGIC   'delta.columnMapping.mode' = 'name'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.TestSchemaMerge;

# COMMAND ----------

chk_df = spark.read.format("delta").load("/delta/mergeschema/")
display(chk_df)

# COMMAND ----------

display(dbutils.fs.ls("/delta/mergeschema/"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Can We Fix It?

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table default.TestSchemaMerge

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Create or replace table default.TestSchemaMerge (
# MAGIC some STRING,
# MAGIC col STRING,
# MAGIC names LONG,
# MAGIC evenewercol STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/delta/mergeschema/'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5',
# MAGIC --   'delta.columnMapping.maxColumnId' = '4',
# MAGIC   'delta.columnMapping.mode' = 'name'
# MAGIC )
