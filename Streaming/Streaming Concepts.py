# Databricks notebook source
# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# Define the schema using a DDL-formatted string.
dataSchema = "Recorded_At timestamp, Device string, Index long, Model string, User string, _corrupt_record String, gt string, x double, y double, z double"

# COMMAND ----------

dataPath = "dbfs:/mnt/training/definitive-guide/data/activity-data-stream.json"
files = dbutils.fs.ls(dataPath)
display(files)

# COMMAND ----------


initialDF = (spark
  .readStream                            # Returns DataStreamReader
  .option("maxFilesPerTrigger", 1)       # Force processing of only 1 file per trigger 
  .schema(dataSchema)                    # Required for all streaming DataFrames
  .json(dataPath)                        # The stream's source directory and file type
)

streamingDF = (initialDF
  .withColumnRenamed("Index", "User_ID")  # Pick a "better" column name
  .drop("_corrupt_record")                # Remove an unnecessary column
)

# Static vs Streaming?
streamingDF.isStreaming

# COMMAND ----------

basePath = userhome + "/structured-streaming-concepts/python" # A working directory for our streaming app
dbutils.fs.mkdirs(basePath)                                   # Make sure that our working directory exists
outputPathDir = basePath + "/output.parquet"                  # A subdirectory for our output
checkpointPath = basePath + "/checkpoint"                     # A subdirectory for our checkpoint & W-A logs

streamingQuery = (streamingDF                                 # Start with our "streaming" DataFrame
  .writeStream                                                # Get the DataStreamWriter
  .queryName("stream_1p")                                     # Name the query
  .trigger(processingTime="3 seconds")                        # Configure for a 3-second micro-batch
  .format("parquet")                                          # Specify the sink type, a Parquet file
  .option("checkpointLocation", checkpointPath)               # Specify the location of checkpoint files & W-A logs
  .outputMode("append")                                       # Write only new data to the "file"
  .start(outputPathDir)                                       # Start the job, writing to the specified directory
)

# Wait until stream is done initializing...
untilStreamIsReady("stream_1p")


# COMMAND ----------

myStream = "stream_1p"
display(streamingDF, streamName = myStream)

# COMMAND ----------

for s in spark.streams.active:         # Iterate over all streams
  print("{}: {}".format(s.id, s.name)) # Print the stream's id and name

# COMMAND ----------

streamingQuery.recentProgress

# COMMAND ----------

streamingQuery.awaitTermination()

# COMMAND ----------

for s in spark.streams.active:
  s.stop()

# COMMAND ----------

myStream = "stream_2p"
display(streamingDF, streamName = myStream)


# COMMAND ----------

for s in spark.streams.active:
  s.stop()

# COMMAND ----------

dbutils.fs.rm(basePath, True)
