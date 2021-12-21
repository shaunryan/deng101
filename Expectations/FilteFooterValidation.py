# Databricks notebook source
# MAGIC %python
# MAGIC 
# MAGIC # not compatible or needed on a passthrough AD cluster!
# MAGIC from fathom.ConnectStorage import connect_storage
# MAGIC from fathom import Configuration as config
# MAGIC 
# MAGIC connect_storage()
# MAGIC config.help()
# MAGIC 
# MAGIC root = config.get_storage_account()

# COMMAND ----------


from pyspark.sql.types import StructType
import json

schema_string = json.loads('{"fields":[{"metadata":{},"name":"ID","nullable":true,"type":"integer"},{"metadata":{},"name":"Case Number","nullable":true,"type":"string"},{"metadata":{},"name":"Date","nullable":true,"type":"string"},{"metadata":{},"name":"Block","nullable":true,"type":"string"},{"metadata":{},"name":"IUCR","nullable":true,"type":"string"},{"metadata":{},"name":"Primary Type","nullable":true,"type":"string"},{"metadata":{},"name":"Description","nullable":true,"type":"string"},{"metadata":{},"name":"Location Description","nullable":true,"type":"string"},{"metadata":{},"name":"Arrest","nullable":true,"type":"boolean"},{"metadata":{},"name":"Domestic","nullable":true,"type":"boolean"},{"metadata":{},"name":"Beat","nullable":true,"type":"integer"},{"metadata":{},"name":"District","nullable":true,"type":"integer"},{"metadata":{},"name":"Ward","nullable":true,"type":"integer"},{"metadata":{},"name":"Community Area","nullable":true,"type":"integer"},{"metadata":{},"name":"FBI Code","nullable":true,"type":"string"},{"metadata":{},"name":"X Coordinate","nullable":true,"type":"integer"},{"metadata":{},"name":"Y Coordinate","nullable":true,"type":"integer"},{"metadata":{},"name":"Year","nullable":true,"type":"integer"},{"metadata":{},"name":"Updated On","nullable":true,"type":"string"},{"metadata":{},"name":"Latitude","nullable":true,"type":"double"},{"metadata":{},"name":"Longitude","nullable":true,"type":"double"},{"metadata":{},"name":"Location","nullable":true,"type":"string"}],"type":"struct"}')

schema = StructType.fromJson(schema_string)


# COMMAND ----------

file = "test_chicagocrime_wf_*_20200101_*.csv"
path = f"{root}raw/test/data/2020/01/01/"
filepath = f"{path}{file}"

config = {
  "header": "true",
  "inferSchema": "false",
  "delimiter": "\t",
  "badRecordsPath": f"{root}/dataengineering/exceptions/chicago_crime_ft/"
}

df = (spark.read.format("csv")
      .schema(schema)
      .options(**config)
      .load(filepath)
      .distinct()
      .count())



print(df)


# COMMAND ----------

from pyspark.sql.functions import expr, col, column

exceptions_config = {
  "inferSchema": "true"
}

df = (spark.read.format("json")
      .options(**exceptions_config)
      .load(f"{root}dataengineering/exceptions/chicago_crime_ft/*/bad_records/*")
      .withColumn("filename", expr("substring_index(substring_index(path, '/', -1), '_', 3)"))
      .withColumn("from_period", expr("to_date(left(substring_index(substring_index(path, '/', -1), '_', -2), 8), 'yMMdd')"))
      .withColumn("to_period", expr("to_date(left(substring_index(substring_index(path, '/', -1), '_', -1), 8), 'yMMdd')"))
      .drop(*["path", "reason"])
      .where("left(record, 14) == 'number_of_rows' and reason == 'org.apache.spark.sql.catalyst.csv.MalformedCSVException: Malformed CSV record'"))

df = (df.withColumn("number_of_rows", expr("from_csv(record, 'number_of_rows STRING, date STRING', map('delimiter', ' ')).number_of_rows"))
        .withColumn("number_of_rows", expr("cast(substring_index(number_of_rows, ':', -1) as int)")))
# df = df.withColumn("number_of_rows", expr("replace(record, ' ', ',') as record"))

#       SELECT from_csv('26/08/2015', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy'));

display(df)
