# Databricks notebook source
from datetime import datetime

# datetime object containing current date and time
now = datetime.now()
 
print("now =", now)

# dd/mm/YY H:M:S
dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
print("date and time =", dt_string)	

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime
from pyspark.sql.functions import expr
from pyspark.sql.types import StructField, StructType, StringType, DateType, IntegerType, TimestampType

peopleSchema = StructType([
  StructField("Name",   StringType(),  True),
  StructField("Family", IntegerType(), True),
  StructField("dob",    TimestampType(),    False)
])
dateformat = "%d-%m-%Y"

rows = [
  Row("Shaun",  1, datetime.now()),
  Row("Sarah",  1, datetime.now()),
  Row("Finley", 1, datetime.now()),
  Row("Paul",   2, datetime.now()),
  Row("Simon",  2, datetime.now())
]

dfp = (spark.createDataFrame(rows, peopleSchema)
      .withColumn("date", expr("date_format(dob, 'yyyy-MM-dd')").cast("Date"))
      .withColumn("time", expr("unix_timestamp(date_format(dob, 'HH:mm:ss'), 'HH:mm:ss')"))
      .withColumn("tstime", expr("from_unixtime(time, 'yyyy-MM-dd HH:mm:ss VV')"))
      )

display(dfp)
