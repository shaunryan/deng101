# Databricks notebook source
# DBTITLE 1,Read Databricks switch action dataset  
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

# DBTITLE 1,Write out DataFrame as Databricks Delta data
events.write.format("delta").mode("overwrite").partitionBy("date").save("/delta/events/")

# COMMAND ----------

# DBTITLE 1,Query the data file path
events_delta = spark.read.format("delta").load("/delta/events/")

display(events_delta)

# COMMAND ----------

# DBTITLE 1,Query a single partition
events_20160726 = spark.read.format("delta").load("/delta/events/").where("date=2016-07-26")

# COMMAND ----------

# DBTITLE 1,Look at file sizes
from operator import itemgetter
files = dbutils.fs.ls("/delta/events/date=2016-07-26")

total_size = sum([f.size for f in files])

file_sizes = [
  {"name":f.name, 
   "size":f.size, 
   "total_size":total_size,
   "percent_size": (f.size / total_size) * 100
  } for f in files]

  
display(file_sizes)


# COMMAND ----------

dbutils.fs.ls("/delta/events/_delta_log/")

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls /dbfs/delta/events/_delta_log/ 
# MAGIC cat /dbfs/delta/events/_delta_log/00000000000000000003.json

# COMMAND ----------

# DBTITLE 1,Repartition partition files
path = "/delta/events/"
partition = "date=2016-07-26"
number_of_files = 1

(spark.read
.format("delta")
.load(path)
.where(partition)
.repartition(number_of_files)
.write
.option("dataChange", "false")
.format("delta")
.mode("overwrite")
.option("replaceWhere", partition)
.save(path))



# COMMAND ----------

from delta.tables import *

# note that in order to see the changes in the files by executing cell 5
# we need to vacuum the files
# To do this however we have to override the safety default to prevent uncommitted files from being vacuumed
# this is fine for a demo to show that the file partitions have in fact been reduced, care should be taken on production however


deltaTable = DeltaTable.forPath(spark, path)

print("before:", spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled"))
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

deltaTable.vacuum(0)

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", True)
print("after:", spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled"))

# COMMAND ----------

from operator import itemgetter
files = dbutils.fs.ls("/delta/events/date=2016-07-26")
display(files)


# COMMAND ----------

# DBTITLE 1,Create table
display(spark.sql("DROP TABLE IF EXISTS events"))

display(spark.sql("CREATE TABLE events USING DELTA LOCATION '/delta/events/'"))

# COMMAND ----------

# DBTITLE 1,Query the table
events_delta.count()

# COMMAND ----------

# DBTITLE 1,Visualize data
from pyspark.sql.functions import count
display(events_delta.groupBy("action","date").agg(count("action").alias("action_count")).orderBy("date", "action"))

# COMMAND ----------

# DBTITLE 1,Generate historical data - original data shifted backwards 2 days
historical_events = spark.read \
  .option("inferSchema", "true") \
  .json("/databricks-datasets/structured-streaming/events/") \
  .withColumn("date", expr("time-172800")) \
  .drop("time") \
  .withColumn("date", from_unixtime("date", 'yyyy-MM-dd'))

# COMMAND ----------

# DBTITLE 1,Append historical data
historical_events.write.format("delta").mode("append").partitionBy("date").save("/delta/events/")

# COMMAND ----------

# DBTITLE 1,Visualize final data
display(events_delta.groupBy("action","date").agg(count("action").alias("action_count")).orderBy("date", "action"))

# COMMAND ----------

# DBTITLE 1,Count rows
events_delta.count()

# COMMAND ----------

# DBTITLE 1,Show contents of a partition
dbutils.fs.ls("dbfs:/delta/events/date=2016-07-25/")

# COMMAND ----------

display(spark.sql("OPTIMIZE events"))

# COMMAND ----------

# DBTITLE 1,Show table history
display(spark.sql("DESCRIBE HISTORY events"))

# COMMAND ----------

# DBTITLE 1,Show table details
display(spark.sql("DESCRIBE DETAIL events"))

# COMMAND ----------

# DBTITLE 1,Show the table format
display(spark.sql("DESCRIBE FORMATTED events"))

# COMMAND ----------

# DBTITLE 1,Clean Up
dbutils.fs.rm("/delta", True)

display(spark.sql("DROP TABLE IF EXISTS events"))
