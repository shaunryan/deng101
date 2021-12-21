# Databricks notebook source
pathToTable = "/delta/events/"

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

events.write.format("delta").mode("overwrite").partitionBy("date").save(pathToTable)

# COMMAND ----------

# DBTITLE 1,Vacuum
from delta.tables import *

# path-based tables
deltaTable = DeltaTable.forPath(spark, pathToTable)

# Hive metastore-based tables
# deltaTable = DeltaTable.forName(spark, tableName)    

# vacuum files not required by versions older than the default retention period
deltaTable.vacuum()        

# vacuum files not required by versions more than 100 hours old

deltaTable.vacuum(100)     


# COMMAND ----------

# DBTITLE 1,Delete files in parallel - false by default
print("before:", spark.conf.get("spark.databricks.delta.vacuum.parallelDelete.enabled"))
spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", True)
print("after:", spark.conf.get("spark.databricks.delta.vacuum.parallelDelete.enabled"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Warning
# MAGIC 
# MAGIC - We do not recommend that you set a retention interval shorter than 7 days
# MAGIC - old snapshots and uncommitted files can still be in use by concurrent readers or writers to the table
# MAGIC - If vacuum cleans up active files, concurrent readers can fail or tables can be corrupted when vacuum deletes files that have not yet been committed.
# MAGIC 
# MAGIC Delta Lake has a safety check to prevent you from running a dangerous vacuum command. 
# MAGIC 
# MAGIC If you are certain that there are no operations being performed on this table that take longer than the retention interval you plan to specify, you can turn off this safety check by setting the Apache Spark configuration property
# MAGIC 
# MAGIC You must choose an interval that is longer than the longest running concurrent transaction and the longest period that any stream can lag behind the most recent update to the table.

# COMMAND ----------

# DBTITLE 1,Rentention Safety
print("before:", spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled"))
