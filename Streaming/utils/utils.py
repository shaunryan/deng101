import json
import yaml
from pyspark.sql.types import StructType
from typing import Union
import os
from pyspark.sql import DataFrame
from uuid import uuid4
from databricks.sdk.runtime import spark, dbutils

def save_schema(schema:Union[StructType,str], name:str, directory="./schema"):

  os.makedirs(directory, exist_ok=True)
  save_ddl = False
  if isinstance(schema, str):
    schema = json.loads(schema)
  elif isinstance(schema, StructType):
    schema = json.loads(schema.json())
  else:
    raise ValueError()

  path = os.path.join(directory, f"{name}.yaml")
  with open(path, "w", encoding="utf-8") as fs:
    fs.write(yaml.safe_dump( schema, indent=4))


def save_ddl(df:DataFrame, name:str, directory:str="./schema"):
  
  os.makedirs(directory, exist_ok=True)
  sql = df._jdf.schema().toDDL()
  path = os.path.join(directory, f"{name}.sql")
  with open(path, "w", encoding="utf-8") as fs:
    fs.write(sql)


def create_inferred_schema(options:dict, format:str, path:str, name:str):

  options["inferSchema"] = True
  options["mode"] = "PERMISSIVE"
  df = spark.read.format(format).options(**options).load(path)
  save_schema(df.schema, name)
  save_ddl(df, name)
  

def add_file(path:str, name:str, root:str, id:str=None, commit=False):

  if not id:
    id = str(uuid4())

  from_path = os.path.abspath(path)
  from_path = f"file:{from_path}"

  filename, ext = os.path.splitext(os.path.basename(from_path))
  filename = f"{name}_{id}{ext}"

  to_path = os.path.join(root, name, filename)

  print(f"copying {from_path} to {to_path}")
  if commit:
    dbutils.fs.cp(from_path, to_path)


def get_source_options(name:str, checkpoint_path:str, config_file:str):

  with open(config_file, "r", encoding="utf-8") as f:
    autoloader_options = yaml.safe_load(f)

  options = (
              autoloader_options["listing"] |
              autoloader_options["formats"]["csv"] | 
              autoloader_options["autoloader"]
            )
  options["cloudFiles.schemaLocation"] = os.path.join(checkpoint_path, "schema")

  if options.get("cloudFiles.schemaHints") and not options.get("cloudFiles.header"):
    schema_path = options["cloudFiles.schemaHints"]
    ddl_schema = load_ddl_schema(schema_path)
    schema_hints = [ f"_c{i} {h.strip().split(' ')[1]}" for i, h in enumerate(ddl_schema.split(","))]
    options["cloudFiles.schemaHints"] = ", ".join(schema_hints)

  return options

def get_destination_options(name:str, checkpoint_path:str, mergeSchema:bool=True):
  options = {
    "checkpointLocation": os.path.join(checkpoint_path, name)
  }
  options["mergeSchema"] = mergeSchema
  return options

def load_schema(filepath:str):

  with open(filepath, "r", encoding="utf-8") as f:
    schema = yaml.safe_load(f)
  
  schema = StructType.fromJson(schema)
  return schema

def load_ddl_schema(filepath:str):

  with open(filepath, "r", encoding="utf-8") as f:
    schema = f.read()
  
  schema = schema.replace("\n", "")
  
  return schema


def create_table(database:str, table:str, location:str):
  spark.sql(f"create database if not exists {database}")
  location = os.path.join(location, table)
  spark.sql(f"""
    create table if not exists {database}.{table}
    using DELTA
    location '{location}'
    -- TBLPROPERTIES (
    --   'spark.databricks.delta.schema.autoMerge.enabled'='true'
    -- )
  """)