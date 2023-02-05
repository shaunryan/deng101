# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Demo for Autoloading
# MAGIC 
# MAGIC Uses cloudfiles and storage events to batch stream data in.
# MAGIC 
# MAGIC ## Setup
# MAGIC 
# MAGIC Clears down and sets up demo.
# MAGIC Copies 1 file into the landing area, sets up the data, checkpoint paths and delta database and table

# COMMAND ----------

from pyspark.sql import functions as fn

numbers = [1]
filename = f"file-"
ext = "json"
database = "events"
table = "events"

data_root_path = f"/databricks-datasets/structured-streaming/{database}"
datalake_root_path = f"/mnt/datalake/data/landing/{database}"
checkpoint_root = f"/mnt/datalake/checkpoint/landing/{database}"
checkpoint_path = f"{checkpoint_root}/{table}"
datalake_path = f"{datalake_root_path}/{table}"
database_table = f"{database}.{table}"
sql_database_table = f"`{database}`.`{table}`"
SQL = f"select source_file, count(1) as row_count from {sql_database_table} group by source_file"

def clear_stream_root(path:str):
  dbutils.fs.rm(path, True)
  dbutils.fs.mkdirs(path)
  spark.sql(f"DROP DATABASE IF EXISTS {database} cascade")
  spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

def copy_to_stream_landing(files:list, from_root:str, to_root:str, filename:str, ext:str="json"):
  
  for n in files:
    print(n)
    from_path = f'{from_root}/{filename}{n}.{ext}'
    to_path = f'{to_root}/{filename}{n}.{ext}'
    print(f"copying {from_path} to {to_path}")
    dbutils.fs.cp(from_path, to_path)

def load_new_data(await_termination:bool=True):
  # Configure Auto Loader to ingest JSON data to a Delta table
  stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .load(datalake_path)
    .select("*", fn.input_file_name().alias("source_file"), fn.current_timestamp().alias("processing_time"))
    .writeStream
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(database_table))
  
  if await_termination:
    stream.awaitTermination()

def check_data_loaded(sql:str=SQL):
  df = spark.sql(SQL)
  display(df)
  
clear_stream_root(checkpoint_root)
clear_stream_root(datalake_root_path)
copy_to_stream_landing(numbers, data_root_path, datalake_path, filename)
files = dbutils.fs.ls(datalake_path)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load File 1

# COMMAND ----------

load_new_data()
check_data_loaded()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load Again
# MAGIC 
# MAGIC Note that the file 1 data isn't loaded again since autoloader checkpointed it.

# COMMAND ----------

load_new_data()
check_data_loaded()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load File 2
# MAGIC 
# MAGIC - Move another file into the landing location
# MAGIC - Execute the load process again
# MAGIC 
# MAGIC Result:
# MAGIC 
# MAGIC - Data from file it incrementally appended to the table and checkpointed

# COMMAND ----------

copy_to_stream_landing([2], data_root_path, datalake_path, filename)
files = dbutils.fs.ls(datalake_path)
display(files)

# COMMAND ----------

load_new_data()
check_data_loaded()
