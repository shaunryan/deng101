# Databricks notebook source
from pyspark.sql import DataFrame

# def read_from_socket():
lines:DataFrame = (spark.readStream
  .format("socket")
  .option("host", "localhost")
  .option("port", 12345)
  .load()
)

display(lines, streamName="socket_stream")

# query = (lines.writeStream
#   .format("memory")
#   .queryName("lines") 
#   .outputMode("append")
#   .start()
# )

# query.awaitTermination()




# COMMAND ----------



# COMMAND ----------

read_from_socket()

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lines
