# Databricks notebook source
# MAGIC %pip install pyyaml

# COMMAND ----------

from utils import utils
import os

# COMMAND ----------

table = "customer_details"

landing_path = "/mnt/datalake/data/landing/autoloader"
bronze_path = "/mnt/datalake/data/raw/autoloader"
table_checkpoint_path = "/mnt/datalake/checkpoint/raw/autoloader"
database = "autoloader"


# COMMAND ----------

from pyspark.sql import functions as fn

def load(
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


utils.create_table(database, table, bronze_path)

source = os.path.join(landing_path, table)
destination = f"{database}.{table}"

source_options = utils.get_source_options(table, table_checkpoint_path, "./config/autoloader_options.yaml")
destination_options = utils.get_destination_options(table, table_checkpoint_path, True)


# COMMAND ----------

load(source, destination, source_options, destination_options)
