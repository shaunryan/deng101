# Databricks notebook source
# MAGIC %sql
# MAGIC Drop table if exists default.test;
# MAGIC create table default.test
# MAGIC (
# MAGIC -- inserting with few columns doesn't work if you have an identity
# MAGIC --   id long GENERATED ALWAYS AS IDENTITY,
# MAGIC   a string,
# MAGIC   b int,
# MAGIC   c boolean
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql import functions as fn
# MAGIC 
# MAGIC schema = StructType([
# MAGIC   StructField("a", StringType(), True),
# MAGIC   StructField("b", IntegerType(), True),
# MAGIC   StructField("c", BooleanType(), True)
# MAGIC ])
# MAGIC 
# MAGIC data = [{"a":"a", "b":1, "c":False},
# MAGIC         {"a":"b", "b":2, "c":True},
# MAGIC         {"a":"c", "b":3, "c":False}]
# MAGIC 
# MAGIC df = spark.createDataFrame(data, schema)
# MAGIC display(df)

# COMMAND ----------

options = {}
result = (
  df.write.format("delta").options(**options).mode("append").saveAsTable(name="default.test")
)

display(result)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.test

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Insert with Fewer Columns

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql import functions as fn
# MAGIC 
# MAGIC schema = StructType([
# MAGIC   StructField("a", StringType(), True),
# MAGIC   StructField("c", BooleanType(), True)
# MAGIC ])
# MAGIC 
# MAGIC data = [{"a":"d", "b":4, "c":False},
# MAGIC         {"a":"e", "b":5, "c":False}]
# MAGIC 
# MAGIC df = spark.createDataFrame(data, schema)
# MAGIC display(df)

# COMMAND ----------

options = {}
result = (
  df.write.format("delta").options(**options).mode("append").saveAsTable(name="default.test")
)

display(result)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.test

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Insert with More Columns

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql import functions as fn
# MAGIC 
# MAGIC schema = StructType([
# MAGIC   StructField("a", StringType(), True),
# MAGIC   StructField("b", IntegerType(), True),
# MAGIC   StructField("c", BooleanType(), True),
# MAGIC   StructField("d", IntegerType(), True)
# MAGIC ])
# MAGIC 
# MAGIC data = [{"a":"f", "b":6, "c":False , "d":1},
# MAGIC         {"a":"g", "b":7, "c":True  , "d":2},
# MAGIC         {"a":"h", "b":8, "c":False , "d":3}]
# MAGIC 
# MAGIC df = spark.createDataFrame(data, schema)
# MAGIC display(df)

# COMMAND ----------

try:
  options = {}
  result = (
    df.write.format("delta").options(**options).mode("append").saveAsTable(name="default.test")
  )

  display(result)
except Exception as e:
  print(e)

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)

options = {}
result = (
  df.write.format("delta").options(**options).mode("append").saveAsTable(name="default.test")
)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", False)
display(result)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.test

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge with Fewer Columns - This will fail

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Method 1: sync the set and values clause

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql import functions as fn
# MAGIC 
# MAGIC schema = StructType([
# MAGIC   StructField("a", StringType(), True),
# MAGIC   StructField("b", IntegerType(), True),
# MAGIC   StructField("d", IntegerType(), True)
# MAGIC ])
# MAGIC 
# MAGIC data = [{"a":"f", "b":60, "d":1},
# MAGIC         {"a":"g", "b":70, "d":2},
# MAGIC         {"a":"h", "b":80, "d":3}]
# MAGIC 
# MAGIC df = spark.createDataFrame(data, schema)
# MAGIC display(df)

# COMMAND ----------

set = { c : f"src.{c}" for c in df.columns}
set

# COMMAND ----------

from delta.tables import DeltaTable

dest_table = DeltaTable.forName(spark, "default.test")

result = (
  dest_table.alias("dst").merge(
     df.alias('src'),
    'dst.a = src.a'
  )
  .whenNotMatchedInsert(values=set)
  .whenMatchedUpdate(condition="dst.b is not null", set=set) # fails with the update
  .whenMatchedDelete()
  .execute()
)
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Method 2: sync the incoming data frame

# COMMAND ----------

# or
from pyspark.sql import functions as fn
from delta.tables import DeltaTable

dest_table = DeltaTable.forName(spark, "default.test")
#Get All column names and it's types

for col in dest_table.toDF().dtypes:
  typ = col[1]
  name = col[0]
  if name not in df.columns:
    df = df.withColumn(name, fn.expr(f"cast(null as {typ})"))

display(df)

# COMMAND ----------

from delta.tables import DeltaTable

dest_table = DeltaTable.forName(spark, "default.test")

result = (
  dest_table.alias("dst").merge(
     df.alias('src'),
    'dst.a = src.a'
  )
  .whenNotMatchedInsertAll()
  .whenMatchedUpdateAll("dst.b is not null") # fails with the update
  .whenMatchedDelete()
  .execute()
)
display(result)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.test

# COMMAND ----------

# MAGIC %md
# MAGIC # Merge with More Columns

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql import functions as fn
# MAGIC 
# MAGIC schema = StructType([
# MAGIC   StructField("f", StringType(), True),
# MAGIC   StructField("a", StringType(), True),
# MAGIC   StructField("b", IntegerType(), True),
# MAGIC   StructField("c", BooleanType(), True),
# MAGIC   StructField("d", IntegerType(), True),
# MAGIC   StructField("e", StringType(), True)
# MAGIC ])
# MAGIC 
# MAGIC data = [{"f":"q", "a":"f", "b":600, "c":False , "d":1, "e":"one"},
# MAGIC         {"f":"w", "a":"g", "b":700, "c":True  , "d":2, "e":"two"},
# MAGIC         {"f":"e", "a":"h", "b":800, "c":False , "d":3, "e":"three"}]
# MAGIC 
# MAGIC df = spark.createDataFrame(data, schema)
# MAGIC display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Method 1: Just Ignore It

# COMMAND ----------

from delta.tables import DeltaTable

dest_table = DeltaTable.forName(spark, "default.test")

result = (
  dest_table.alias("dst").merge(
     df.alias('src'),
    'dst.a = src.a'
  )
  .whenNotMatchedInsertAll()
  .whenMatchedUpdateAll("dst.b is not null") # fails with the update
  .whenMatchedDelete()
  .execute()
)
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Method 2: Auto Merge

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)

from delta.tables import DeltaTable

dest_table = DeltaTable.forName(spark, "default.test")

result = (
  dest_table.alias("dst").merge(
     df.alias('src'),
    'dst.a = src.a'
  )
  .whenNotMatchedInsertAll()
  .whenMatchedUpdateAll("dst.b is not null") # fails with the update
  .whenMatchedDelete()
  .execute()
)
display(result)

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", False)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Method 3: Alter Table

# COMMAND ----------

add_columns = []
from delta.tables import DeltaTable
dest_table = DeltaTable.forName(spark, "default.test")
prec_col = None

for col in df.dtypes:
  typ = col[1]
  name = col[0]
  if name not in dest_table.toDF().columns:
    if prec_col:
      add_columns.append(f"`{name}` {typ} after {prec_col}")
    else:
      add_columns.append(f"`{name}` {typ} first")
  prec_col = name

if add_columns:
  add_columns = ",".join(add_columns)
  sql = f"""
    alter table default.test
    add columns (
      {add_columns}
    )
  """

  print(sql)
  spark.sql(sql)

# COMMAND ----------

dest_table = DeltaTable.forName(spark, "default.test")

result = (
  dest_table.alias("dst").merge(
     df.alias('src'),
    'dst.a = src.a'
  )
  .whenNotMatchedInsertAll()
  .whenMatchedUpdateAll("dst.b is not null") # fails with the update
  .whenMatchedDelete()
  .execute()
)
display(result)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from default.test

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean Up

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table if exists default.test
