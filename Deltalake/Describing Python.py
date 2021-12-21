# Databricks notebook source
path = "/delta/events/"

# COMMAND ----------

from pyspark.sql.functions import expr
from pyspark.sql.functions import from_unixtime

events = spark.read \
  .option("inferSchema", "true") \
  .json("/databricks-datasets/structured-streaming/events/") \
  .withColumn("date", expr("time")) \
  .drop("time") \
  .withColumn("date", from_unixtime("date", 'yyyy-MM-dd'))
  
display(events)

# COMMAND ----------

events.write.format("delta").mode("overwrite").partitionBy("date").save(path)

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, path)

full_history = deltaTable.history()
display(full_history)

lastOperationDF = deltaTable.history(1)
