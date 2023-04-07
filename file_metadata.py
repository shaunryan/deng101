# Databricks notebook source
dbutils.fs.ls("/mnt/datalake/data/landing/demo/20210101/*")

# COMMAND ----------

from pyspark.sql.types import StructType
import json

json_string = '{"fields":[{"metadata":{},"name":"_corrupt_record","nullable":true,"type":"string"},{"metadata":{},"name":"allow_contact","nullable":true,"type":"boolean"},{"metadata":{},"name":"amount","nullable":true,"type":"double"},{"metadata":{},"name":"email","nullable":true,"type":"string"},{"metadata":{},"name":"first_name","nullable":true,"type":"string"},{"metadata":{},"name":"gender","nullable":true,"type":"string"},{"metadata":{},"name":"id","nullable":true,"type":"long"},{"metadata":{},"name":"job_title","nullable":true,"type":"string"},{"metadata":{},"name":"last_name","nullable":true,"type":"string"}],"type":"struct"}'
json_schema = json.loads(json_string)

schema = StructType.fromJson(json_schema)

# COMMAND ----------

df = (spark.read 
  .format("json") 
  .options(**{"inferschema":True})
  .schema(schema) 
  .load("/mnt/datalake/data/landing/demo/20210101/*")
  .select("_metadata.*")
)

display(df)
#   


# COMMAND ----------

df.schema.json()
