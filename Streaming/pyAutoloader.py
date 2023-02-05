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
roots = [checkpoint_root, datalake_root_path]
checkpoint_path = f"{checkpoint_root}/{table}"
datalake_path = f"{datalake_root_path}/{table}"
database_table = f"{database}.{table}"
sql_database_table = f"`{database}`.`{table}`"
SQL = f"select source_file, count(1) as row_count from {sql_database_table} group by source_file"
source_options = {
  "cloudFiles.format": ext,
  "cloudFiles.schemaLocation": checkpoint_path
}
dest_options = {
  "checkpointLocation": checkpoint_path
}

def clear_demo(paths:list, database:str):
  for p in paths:
    dbutils.fs.rm(p, True)
    dbutils.fs.mkdirs(p)
  spark.sql(f"DROP DATABASE IF EXISTS {database} cascade")
  spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

def copy_to_stream_landing(files:list, from_root:str, to_root:str, filename:str, ext:str="json"):
  
  for n in files:
    print(n)
    from_path = f'{from_root}/{filename}{n}.{ext}'
    to_path = f'{to_root}/{filename}{n}.{ext}'
    print(f"copying {from_path} to {to_path}")
    dbutils.fs.cp(from_path, to_path)

def load_new_data(
  source:str, 
  destination:str, 
  source_options:dict, 
  dest_options:dict, 
  await_termination:bool=True
):
  # Configure Auto Loader to ingest JSON data to a Delta table
  stream = (spark.readStream
    .format("cloudFiles")
    .options(**source_options)
    .load(source)
    .select("*", 
      fn.input_file_name().alias("source_file"), 
      fn.current_timestamp().alias("processing_time")
     )
    .writeStream
    .options(**dest_options)
    .trigger(availableNow=True)
    .toTable(destination))
  
  # awaiting the stream will block until the stream ended
  # with availableNow trigger the stream will end when all the files
  # that haven't been processed yet are processed
  if await_termination:
    stream.awaitTermination()

def check_data_loaded(sql:str=SQL):
  df = spark.sql(SQL)
  display(df)
  
clear_demo(roots, database)
copy_to_stream_landing(numbers, data_root_path, datalake_path, filename)
files = dbutils.fs.ls(datalake_path)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load File 1

# COMMAND ----------

load_new_data(datalake_path, database_table, source_options, dest_options)
check_data_loaded()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load Again
# MAGIC 
# MAGIC Note that the file 1 data isn't loaded again since autoloader checkpointed it.

# COMMAND ----------

load_new_data(datalake_path, database_table, source_options, dest_options)
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
# MAGIC - Data from the file is incrementally appended to the table and checkpointed

# COMMAND ----------

copy_to_stream_landing([2], data_root_path, datalake_path, filename)
files = dbutils.fs.ls(datalake_path)
display(files)

# COMMAND ----------

load_new_data(datalake_path, database_table, source_options, dest_options)
check_data_loaded()
