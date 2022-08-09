# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC -- create table
# MAGIC Drop table if exists default.test;
# MAGIC create table default.test
# MAGIC (
# MAGIC -- inserting with fewer columns doesn't work if you have a generated identity
# MAGIC --   id long GENERATED ALWAYS AS IDENTITY,
# MAGIC   a string,
# MAGIC   b int,
# MAGIC   c boolean
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Setup
# MAGIC 
# MAGIC ### Normal Insert

# COMMAND ----------



from pyspark.sql.types import *
from pyspark.sql import functions as fn

schema = StructType([
  StructField("a", StringType(), True),
  StructField("b", IntegerType(), True),
  StructField("c", BooleanType(), True)
])

data = [{"a":"a", "b":1, "c":False},
        {"a":"b", "b":2, "c":True},
        {"a":"c", "b":3, "c":False}]

df = spark.createDataFrame(data, schema)
display(df)

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
# MAGIC # Insert with Fewer Columns - Works Fine

# COMMAND ----------



from pyspark.sql.types import *
from pyspark.sql import functions as fn

schema = StructType([
  StructField("a", StringType(), True),
  StructField("c", BooleanType(), True)
])

data = [{"a":"d", "b":4, "c":False},
        {"a":"e", "b":5, "c":False}]

df = spark.createDataFrame(data, schema)
display(df)

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
# MAGIC # Insert with More Columns - this will fail

# COMMAND ----------



from pyspark.sql.types import *
from pyspark.sql import functions as fn

schema = StructType([
  StructField("a", StringType(), True),
  StructField("b", IntegerType(), True),
  StructField("c", BooleanType(), True),
  StructField("d", IntegerType(), True)
])

data = [{"a":"f", "b":6, "c":False , "d":1},
        {"a":"g", "b":7, "c":True  , "d":2},
        {"a":"h", "b":8, "c":False , "d":3}]

df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Method 1: See merge options below cmd17

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

# MAGIC %md
# MAGIC 
# MAGIC ### Merge 2: auto merge

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
# MAGIC # Merge with Fewer Columns - This will fail see methods to resolve

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Method 1: sync the set and values clause

# COMMAND ----------


from pyspark.sql.types import *
from pyspark.sql import functions as fn

schema = StructType([
  StructField("a", StringType(), True),
  StructField("b", IntegerType(), True),
  StructField("d", IntegerType(), True)
])

data = [{"a":"f", "b":60, "d":1},
        {"a":"g", "b":70, "d":2},
        {"a":"h", "b":80, "d":3}]

df = spark.createDataFrame(data, schema)
display(df)

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
  .whenMatchedUpdateAll("dst.b is not null")
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


from pyspark.sql.types import *
from pyspark.sql import functions as fn

schema = StructType([
  StructField("f", StringType(), True),
  StructField("a", StringType(), True),
  StructField("b", IntegerType(), True),
  StructField("c", BooleanType(), True),
  StructField("d", IntegerType(), True),
  StructField("e", StringType(), True)
])

data = [{"f":"q", "a":"f", "b":600, "c":False , "d":1, "e":"one"},
        {"f":"w", "a":"g", "b":700, "c":True  , "d":2, "e":"two"},
        {"f":"e", "a":"h", "b":800, "c":False , "d":3, "e":"three"}]

df = spark.createDataFrame(data, schema)
display(df)

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
