# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Displaying Configurations

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.SparkSession
# MAGIC 
# MAGIC def printConfigs(session: SparkSession) = {
# MAGIC   
# MAGIC   val mconf = session.conf.getAll
# MAGIC 
# MAGIC   val cells = mconf.keySet.map( k => s"$k -> ${mconf(k)}")
# MAGIC   print(cells.mkString("\n"))
# MAGIC   
# MAGIC }
# MAGIC 
# MAGIC printConfigs(spark)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC display(spark.sql("SET -v").select("key", "value"))

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC display(spark.sql("SET -v").select("key", "value"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Setting or Modifying Existing

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # scala and python!
# MAGIC print(spark.conf.isModifiable("spark.sql.shuffle.partitions"))
# MAGIC print(spark.conf.get("spark.sql.shuffle.partitions"))
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", 5)
# MAGIC print(spark.conf.get("spark.sql.shuffle.partitions"))
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", 200)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Tuning
# MAGIC 
# MAGIC - https://spark.apache.org/docs/latest/configuration.html#dynamic-allocation
# MAGIC - https://towardsdatascience.com/how-does-facebook-tune-apache-spark-for-large-scale-workloads-3238ddda0830

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### spark.dynamicAllocation
# MAGIC 
# MAGIC 
# MAGIC - By default `spark.dynamicAllocation.enabled` is set to false. 
# MAGIC - When enabled with the settings shown here, the Spark driver will request that the cluster manager create two executors to start with, as a minimum (`spark.dynamicAllocation.minExecutors`). 
# MAGIC - As the task queue backlog increases, new executors will be requested each time the backlog timeout (`spark.dynamicAllocation.schedulerBacklogTimeout`) is exceeded. 
# MAGIC - In this case, whenever there are pending tasks that have not been scheduled for over 1 minute, the driver will request that a new executor be launched to schedule backlogged tasks, up to a maximum of 20 (`spark.dynamicAllocation.maxExecutors`). 
# MAGIC - By contrast, if an executor finishes a task and is idle for 2 minutes (`spark.dynamicAllocation.executorIdleTimeout`), the Spark driver will terminate it.

# COMMAND ----------

# print(spark.conf.set("spark.dynamicAllocation.enabled", True))
# print(spark.conf.set("spark.dynamicAllocation.minExecutors", 2))
# print(spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "1m"))
# print(spark.conf.set("spark.dynamicAllocation.maxExecutors", 20))
# print(spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "2min"))

# COMMAND ----------

# print(spark.conf.get("spark.dynamicAllocation.enabled"))
# print(spark.conf.get("spark.dynamicAllocation.minExecutors"))
# print(spark.conf.get("spark.dynamicAllocation.schedulerBacklogTimeout"))
# print(spark.conf.get("spark.dynamicAllocation.maxExecutors"))
# print(spark.conf.get("spark.dynamicAllocation.executorIdleTimeout"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Memory Configuration

# COMMAND ----------

# print(spark.conf.get("spark.driver.memory"))
# print(spark.conf.get("spark.shuffle.file.buffer"))
# print(spark.conf.get("spark.file.transferTo"))
# print(spark.conf.get("spark.shuffle.unsafe.file.output.buffer"))
# print(spark.conf.get("spark.io.compression.lz4.blockSize"))
# print(spark.conf.get("spark.shuffle.service.index.cache.size"))
# print(spark.conf.get("spark.shuffle.registration.timeout"))
# print(spark.conf.get("spark.shuffle.registration.maxAttempts"))

# COMMAND ----------


print(spark.conf.get("spark.sql.files.maxPartitionBytes"))

# The default value for spark.sql.shuffle.partitions is too high for smaller or streaming workloads; 
# you may want to reduce it to a lower value such as the number of cores on the executors or less.
print(spark.conf.get("spark.sql.shuffle.partitions"))


# Created during operations like groupBy() or join(), also known as wide transformations, shuffle partitions consume both network and disk I/O resources. 
# During these operations, the shuffle will spill results to executors’ local disks at the location specified in spark.local.directory. 
# Having performant SSD disks for this operation will boost the performance.
# print(spark.conf.get("spark.local.directory"))
