# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Joins
# MAGIC 
# MAGIC - trigger a large amount of data movement
# MAGIC - At the heart of these transformations is how Spark computes what data to produce, what keys and associated data to write to the disk, and how to transfer those keys and data to nodes as part of operations like groupBy(), join(), agg(), sortBy(), and reduceByKey(). This movement is commonly referred to as the shuffle.
# MAGIC 
# MAGIC Spark has 5 ways of joining:
# MAGIC 1. Broadcast Hash Join (BHJ or map side only join)
# MAGIC 2. Shuffle Hash Join (SHJ)
# MAGIC 3. Broadcast Nested Loop Join (BNLJ)
# MAGIC 4. Shuffle-&-Replicated Nested Loop Join (Cartesian Product Join)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Broadcast Join
# MAGIC 
# MAGIC NOTE
# MAGIC In this code we are forcing Spark to do a broadcast join, but it will resort to this type of join by default if the size of the smaller data set is below the 
# MAGIC `spark.sql.autoBroadcastJoinThreshold = 10MB`

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // BHJ
# MAGIC import org.apache.spark.sql.functions.broadcast
# MAGIC 
# MAGIC val joinedDF = playersDF.join(broadcast(clubsDF), "key1 === key2")

# COMMAND ----------

from pyspark.sql.functions import broadcast

joinedDF = playersDF.join(broadcast(clubsDF), "key1 = key2")
