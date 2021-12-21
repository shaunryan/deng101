# Databricks notebook source
# load up the configuration and see what there is
from azure_utils.Configuration import *
help()

# COMMAND ----------

from enum import Enum



class DataPath(Enum):
    LANDING = 1
    SCHEMA = 2
    DELTA = 3

    
    
def read_json_file(path:str):

  schemadefinition = spark.read \
    .option("multiLine", "true") \
    .option("mode", "PERMISSIVE") \
    .option("delimiter", "\n") \
    .csv(path)

  schemaJson = [line[0] for line in schemadefinition.collect()]
  
  return '\n'.join(schemaJson)



def get_data_path(config:dict, datapath:DataPath):
    
  storage_account = config.get("storage_account")
  schema_root = config.get("schema_root")
  delta_root = config.get("delta_root")
  layer = config.get("layer")
  database = config.get("database")
  table = config.get("table")
  format = config.get("format")
  
  if datapath == DataPath.LANDING:
    
    return f'{storage_account}{layer}/{database}/{table}.{format}'
  
  elif datapath == DataPath.SCHEMA:
    
    return f'{storage_account}{schema_root}/{layer}.{database}/spark.{layer}.{database}.{table}.{format}'
  
  elif datapath == DataPath.DELTA:
    
    return f"{storage_account}{delta_root}{layer}/{database}/{table}"

  
  
def create_table(config:dict, drop=False):
  
  database = config.get("database")
  table = config.get("table")
  destination_path = get_data_path(config, DataPath.DELTA)
  
  if drop:
    spark.sql(f"DROP TABLE IF EXISTS {database}.{table}")
  
  spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
  spark.sql(f"CREATE TABLE IF NOT EXISTS {database}.{table} USING DELTA LOCATION '{destination_path}'")


# COMMAND ----------


import yaml

pipline_path = f"{get_storage_account()}/pipeline/pipes/pipeline.raw.deltaplay.family.yml"
pipeline = yaml.safe_load(read_json_file(pipline_path))

source_path = get_data_path(pipeline["source"], DataPath.LANDING)
source_schema_path = get_data_path(pipeline["source"], DataPath.SCHEMA)
destination_path = get_data_path(pipeline["destination"], DataPath.DELTA)

print(f"""
  Source: \t\t {source_path}
  Source Schema: \t {source_schema_path}
  Destination: \t\t {destination_path}
  Destination Merge: \t {pipeline["destination"]["merge"]}
  Destination Mode: \t {pipeline["destination"]["mode"]}
""")


# COMMAND ----------

from pyspark.sql.types import StructType
import json

schema_json = json.loads(read_json_file(source_schema_path))
spark_schema = StructType.fromJson(schema_json)

raw = spark.read.json(source_path, spark_schema)

format = pipeline['destination']['format']
mode = pipeline['destination']['mode']
merge = pipeline['destination']['merge']

raw.write.format(format) \
  .mode(mode) \
  .option("mergeSchema", merge) \
  .save(destination_path)

# COMMAND ----------

create_table(pipeline["destination"])

# COMMAND ----------


database = pipeline['destination']['database']
table = pipeline['destination']['table']
df = spark.sql(f"select * from {database}.{table}")
display(df)

