# Databricks notebook source
userhome = "dbfs:/shaunryan"

basePath = userhome + "/streaming" # A working directory for our streaming app
dbutils.fs.mkdirs(basePath)                                   # Make sure that our working directory exists
outputPathDir = basePath + "/output.parquet"                  # A subdirectory for our output
checkpointPath = basePath + "/checkpoint"       

# COMMAND ----------

from pyspark.sql.functions import *
lines = (spark
         .readStream.format("socket")
         .option("host", "localhost")
         .option("port", 9999)
         .load()
)

words = lines.select(split(col("value"), "\\s").alias("word"))
counts = words.groupBy("word").count()

streamingQuery = (
counts
  .writeStream
  .format("console")
  .outputMode("complete")
  .trigger(processingTime="1 second")
  .option("checkpointLocation", checkpointPath)
  .start()                
)

streamingQuery.awaitTermination()

# COMMAND ----------


