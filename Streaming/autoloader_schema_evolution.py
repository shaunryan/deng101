# Databricks notebook source
# MAGIC %pip install pyyaml

# COMMAND ----------

from utils import utils
import os

# COMMAND ----------

paths = {
  "customer_details": "./data/customer_details.csv",
  "customer_preferences": "./data/customer_preferences.csv"
}


landing_path = "/mnt/datalake/data/landing/autoloader"
bronze_path = "/mnt/datalake/data/raw/autoloader"
table_checkpoint_path = "/mnt/datalake/checkpoint/raw/autoloader"
database = "autoloader"



def clear_down(database:str):
  spark.sql(f"drop database if exists {database} cascade")
  dbutils.fs.rm(landing_path, True)
  dbutils.fs.rm(bronze_path, True)
  dbutils.fs.rm(table_checkpoint_path, True)

clear_down("autoloader")

# COMMAND ----------

# Create the schema

# options = {
#   "header": True,
# }
# for name, path in paths.items():
#   data_path = os.path.abspath(path)
#   data_path = f"file:{data_path}"
#   utils.create_inferred_schema(options, "csv", data_path, name)

# COMMAND ----------

from pyspark.sql import functions as fn

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
      fn.current_timestamp().alias("_load_timestamp"),
      "_metadata.*"
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

# COMMAND ----------

# drop data file into landing
import os
table = "customer_details"
path = paths[table]
utils.add_file(name=table, path=path, root=landing_path, commit=True)
utils.create_table(database, table, bronze_path)

source = os.path.join(landing_path, table)
destination = f"{database}.{table}"



# COMMAND ----------

source_options = utils.get_source_options(table, table_checkpoint_path, "./autoloader_options.yaml")
destination_options = utils.get_destination_options(table, table_checkpoint_path, True)

load_new_data(source, destination, source_options, destination_options)

# COMMAND ----------

df = spark.sql(f"""
  select *
  from {database}.{table}
""")
display(df)

# COMMAND ----------

files = dbutils.fs.ls(source)
