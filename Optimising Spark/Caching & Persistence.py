# Databricks notebook source
# MAGIC %md
# MAGIC # What is the difference between caching and persistence? 
# MAGIC 
# MAGIC - In Spark they are synonymous. 
# MAGIC - Two API calls, `cache()` and `persist()`, offer these capabilities
# MAGIC - `persist()` provides more control over how and where your data is stored—in memory and on disk, serialized and unserialized
# MAGIC - Both contribute to better performance for frequently accessed DataFrames or tables.
# MAGIC 
# MAGIC As a general rule you should use memory caching judiciously, as it can incur resource costs in serializing and deserializing, depending on the `StorageLevel` used.
# MAGIC 
# MAGIC ## When to Cache and Persist
# MAGIC - access a large data set repeatedly for queries or transformations
# MAGIC - e.g. DataFrames commonly used during iterative machine learning training
# MAGIC - e.g. DataFrames accessed commonly for doing frequent transformations during ETL or building data pipelines
# MAGIC 
# MAGIC ## When Not to Cache and Persist
# MAGIC Not all use cases dictate the need to cache. Some scenarios that may not warrant caching your DataFrames include:
# MAGIC 
# MAGIC - DataFrames that are too big to fit in memory
# MAGIC - An inexpensive transformation on a DataFrame not requiring frequent use, regardless of size

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # DataFrame.cache()
# MAGIC 
# MAGIC - `cache()` will store as many of the partitions read in memory across Spark executors as memory allows (see Figure 7-2). 
# MAGIC - While a DataFrame may be fractionally cached, partitions cannot be fractionally cached (e.g., if you have 8 partitions but only 4.5 partitions can fit in memory, only 4 will be cached). 
# MAGIC - However, if not all your partitions are cached, when you want to access the data again, the partitions that are not cached will have to be recomputed, slowing down your Spark job.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val df = spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id")
# MAGIC df.cache() // cache the data
# MAGIC df.count() //Materialize the cache
# MAGIC 
# MAGIC df.count()

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val df = spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id")
# MAGIC df.cache() // cache the data
# MAGIC df.count() //Materialize the cache

# COMMAND ----------

# MAGIC %scala
# MAGIC df.count()

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC df.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # DataFrame.persist(StorageLevel.LEVEL)
# MAGIC 
# MAGIC NOTE:
# MAGIC 
# MAGIC Each StorageLevel (except OFF_HEAP) has an equivalent LEVEL_NAME_2, which means replicate twice on two different Spark executors: MEMORY_ONLY_2, MEMORY_AND_DISK_SER_2, etc. While this option is expensive, it allows data locality in two places, providing fault tolerance and giving Spark the option to schedule a task local to a copy of the data.
# MAGIC 
# MAGIC |StorageLevel	        | Description |
# MAGIC |-----------------------|-------------|
# MAGIC |MEMORY_ONLY	        | Data is stored directly as objects and stored only in memory. |
# MAGIC |MEMORY_ONLY_SER	    | Data is serialized as compact byte array representation and stored only in memory. To use it, it has to be deserialized at a cost.| 
# MAGIC |MEMORY_AND_DISK	    | Data is stored directly as objects in memory, but if there’s insufficient memory the rest is serialized and stored on disk. |
# MAGIC |DISK_ONLY	            | Data is serialized and stored on disk. |
# MAGIC |OFF_HEAP	            | Data is stored off-heap. Off-heap memory is used in Spark for storage and query execution; see “Configuring Spark executors’ memory and the shuffle service”. |
# MAGIC |MEMORY_AND_DISK_SER	| Like MEMORY_AND_DISK, but data is serialized when stored in memory. (Data is always serialized when stored on disk.) |

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // In Scala
# MAGIC import org.apache.spark.storage.StorageLevel
# MAGIC 
# MAGIC // Create a DataFrame with 10M records
# MAGIC val df = spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id")
# MAGIC df.persist(StorageLevel.DISK_ONLY) // Serialize the data and cache it on disk
# MAGIC df.count() // Materialize the cache

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC df.count() // Now get it from the cache 

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC df.unpersist() // Now get it from the cache 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Cache Using SQL

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC df.createOrReplaceTempView("dfTable")
# MAGIC spark.sql("CACHE TABLE dfTable")
# MAGIC spark.sql("SELECT count(*) FROM dfTable").show()
