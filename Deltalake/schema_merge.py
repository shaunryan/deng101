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

from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType

schema = StructType([
  StructField("some", StringType(), True),
  StructField("col", StringType(), True),
  StructField("names", LongType(), False)
])

rows = [Row("One", "A", 1), 
          Row("Two", "B", 2), 
          Row("Three", "C", 3)]

src_df = spark.createDataFrame(rows, schema)

(src_df.write
  .format("delta")
  .option("mergeSchema", "true")
  .mode("append")
  .saveAsTable("default.TestSchemaMerge"))
#   .save("/delta/mergeschema/"))

chk_df = spark.read.format("delta").load("/delta/mergeschema/")
display(chk_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create A Change Schema
# MAGIC 
# MAGIC - New ohno column added in the middle
# MAGIC - Old col removed

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType

schema = StructType([
  StructField("some", StringType(), True),
#   StructField("col", StringType(), True),
  StructField("ohno", StringType(), True),
  StructField("names", LongType(), False)
])

rows = [Row("One", "tada", 1), 
        Row("Two", "check", 2), 
        Row("Three", "me", 3)]

src_df = spark.createDataFrame(rows, schema)
display(src_df)

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

# MAGIC %md
# MAGIC 
# MAGIC The data is still there!

# COMMAND ----------

display(dbutils.fs.ls("/delta/mergeschema/"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Can We Fix It? NO!

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

# COMMAND ----------

# MAGIC %sql
# MAGIC -- unsetting doesn't work
# MAGIC ALTER TABLE default.TestSchemaMerge
# MAGIC UNSET TBLPROPERTIES ( 'delta.columnMapping.mode')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- can't be changed either
# MAGIC ALTER TABLE default.TestSchemaMerge
# MAGIC SET TBLPROPERTIES ( 'delta.columnMapping.mode' = 'id')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- can't be changed either
# MAGIC ALTER TABLE default.TestSchemaMerge
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '4'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Show History

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe history default.TestSchemaMerge
