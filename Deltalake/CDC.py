# Databricks notebook source

table_name = "name_job"
path = "/delta/default/cdc_demo"

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType

schema = StructType([
  StructField("name", StringType(), True),
  StructField("job", StringType(), True),
])

rows = [{"name": "Rob", "job":"sweeper"},
          {"name": "Ben", "job":"cook"},
          {"name": "Jen", "job":"gardener"}]

df = spark.createDataFrame(rows, schema)
df.write.format("delta").mode("overwrite").saveAsTable(table_name, path=path)

# COMMAND ----------

df = (spark.read.format("delta") 
    .option("readChangeFeed", "true") 
    .option("startingVersion", 0) 
    .table(table_name))

display(df)

# COMMAND ----------

spark.sql(f"update {table_name} set job = 'washer' where name = 'Ben'")

# COMMAND ----------

df = (spark.read.format("delta") 
    .option("readChangeFeed", "true") 
    .option("startingVersion", 0) 
    .table(table_name))

display(df)

# COMMAND ----------

df = (spark.readStream.format("delta")
     .option("readChangeFeed", "true")
#     .option("startingVersion", 0)
     .table(table_name))

display(df)

# COMMAND ----------

df = spark.sql(f"SHOW TBLPROPERTIES {table_name}")
display(df)

# COMMAND ----------

spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES(delta.enableChangeDataFeed = false)")
df = spark.sql(f"SHOW TBLPROPERTIES {table_name}")
display(df)

# COMMAND ----------

spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES(delta.enableChangeDataFeed = true)")
df = spark.sql(f"SHOW TBLPROPERTIES {table_name}")
display(df)

# COMMAND ----------

df = (spark.read.format("delta") 
    .option("readChangeFeed", "true") 
    .option("startingVersion", 0) 
    .table(table_name))

display(df)

# COMMAND ----------

df = (spark.read.format("delta") 
#     .option("readChangeFeed", "true") 
#     .option("startingVersion", 0) 
    .table(table_name))

display(df)

# COMMAND ----------

dbutils.fs.rm(path, True)
spark.sql(f"drop table if exists {table_name}")
