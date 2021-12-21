# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### JDBC Driver
# MAGIC 
# MAGIC Need to specify driver and needs to be on the spark class path.

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC # From the spark home folder execute the following
# MAGIC 
# MAGIC ./bin/spark-shell --driver-class-path $database.jar --jars $database.jar

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Connection properties - [full list](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#jdbc-to-other-databases)
# MAGIC 
# MAGIC | Property Name      | Description                                                                             |
# MAGIC |--------------------|-----------------------------------------------------------------------------------------|
# MAGIC | user               | These are normally provided as connection properties for logging into the data sources. |
# MAGIC | password           | These are normally provided as connection properties for logging into the data sources. |
# MAGIC | url                | JDBC connection URL, e.g., jdbc:postgresql://localhost/test?user=fred&password=secret.  |
# MAGIC | dbtable            | JDBC table to read from or write to. You can’t specify the dbtable and query options at the same time.|
# MAGIC | query              | Query to be used to read data from Apache Spark, e.g., `SELECT column1, column2, ..., columnN FROM table or subquery`. You can’t specify the query and dbtable options at the same time.|
# MAGIC | driver             |Class name of the JDBC driver to use to connect to the specified URL. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partitioning
# MAGIC 
# MAGIC Best practice:
# MAGIC  - For large amounts of data is important to partition your data source
# MAGIC  - All data is going through one driver connection which cansaturate and slow down and saturate the source system resources
# MAGIC  - The following properties are highly recommended as best practive
# MAGIC  
# MAGIC | Property Name      | Description                                                                             |
# MAGIC |--------------------|-----------------------------------------------------------------------------------------|
# MAGIC | numPartitions      | The maximum number of partitions that can be used for parallelism in table reading and writing. This also determines the maximum number of concurrent JDBC connections. |
# MAGIC | partitionColumn    | When reading an external source, partitionColumn is the column that is used to determine the partitions; note, partitionColumn must be a numeric, date, or timestamp column. |
# MAGIC | lowerBound         | Sets the minimum value of partitionColumn for the partition stride. |
# MAGIC | upperBound         | Sets the maximum value of partitionColumn for the partition stride.|
# MAGIC 
# MAGIC 
# MAGIC Let’s take a look at an example to help you understand how these properties work. Suppose we use the following settings:
# MAGIC ```
# MAGIC numPartitions: 10
# MAGIC lowerBound: 1000
# MAGIC upperBound: 10000
# MAGIC ```
# MAGIC Then the stride is equal to 1,000, and 10 partitions will be created. This is the equivalent of executing these 10 queries (one for each partition):
# MAGIC 
# MAGIC ```
# MAGIC SELECT * FROM table WHERE partitionColumn BETWEEN 1000 and 2000
# MAGIC SELECT * FROM table WHERE partitionColumn BETWEEN 2000 and 3000
# MAGIC ...
# MAGIC SELECT * FROM table WHERE partitionColumn BETWEEN 9000 and 10000
# MAGIC ```
# MAGIC 
# MAGIC Some hints using these properties:
# MAGIC 
# MAGIC A good starting point for numPartitions is to use a multiple of the number of Spark workers. For example, if you have four Spark worker nodes, then perhaps start with 4 or 8 partitions. But it is also important to note how well your source system can handle the read requests. For systems that have processing windows, you can maximize the number of concurrent requests to the source system; for systems lacking processing windows (e.g., an OLTP system continuously processing data), you should reduce the number of concurrent requests to prevent saturation of the source system.
# MAGIC 
# MAGIC Initially, calculate the lowerBound and upperBound based on the minimum and maximum partitionColumn actual values. For example, if you choose {numPartitions:10, lowerBound: 1000, upperBound: 10000}, but all of the values are between 2000 and 4000, then only 2 of the 10 queries (one for each partition) will be doing all of the work. In this scenario, a better configuration would be {numPartitions:10, lowerBound: 2000, upperBound: 4000}.
# MAGIC 
# MAGIC Choose a partitionColumn that can be uniformly distributed to avoid data skew. For example, if the majority of your partitionColumn has the value 2500, with {numPartitions:10, lowerBound: 1000, upperBound: 10000} most of the work will be performed by the task requesting the values between 2000 and 3000. Instead, choose a different partitionColumn, or if possible generate a new one (perhaps a hash of multiple columns) to more evenly distribute your partitions.
