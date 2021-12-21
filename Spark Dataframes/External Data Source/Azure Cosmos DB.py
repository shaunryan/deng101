# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Azure Cosmos DB
# MAGIC 
# MAGIC [GitHub](https://github.com/Azure/azure-cosmosdb-spark)</br>
# MAGIC [Maven](https://search.maven.org/search?q=azure-cosmosdb-spark)
# MAGIC ```
# MAGIC bin/spark-shell --jars postgresql-42.2.6.jar
# MAGIC ```
# MAGIC 
# MAGIC You also have the option of using --packages to pull the connector from Spark Packages using its Maven coordinates:
# MAGIC ```
# MAGIC export PKG="com.microsoft.azure:azure-cosmosdb-spark_2.4.0_2.11:1.3.5"
# MAGIC bin/spark-shell --packages $PKG
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cosmos Scala

# COMMAND ----------

# MAGIC %scala
# MAGIC // In Scala
# MAGIC // Import necessary libraries
# MAGIC import com.microsoft.azure.cosmosdb.spark.schema._
# MAGIC import com.microsoft.azure.cosmosdb.spark._
# MAGIC import com.microsoft.azure.cosmosdb.spark.config.Config
# MAGIC 
# MAGIC // Loading data from Azure Cosmos DB
# MAGIC // Configure connection to your collection
# MAGIC val query = "SELECT c.colA, c.coln FROM c WHERE c.origin = 'SEA'"
# MAGIC val readConfig = Config(Map(
# MAGIC   "Endpoint" -> "https://[ACCOUNT].documents.azure.com:443/", 
# MAGIC   "Masterkey" -> "[MASTER KEY]",
# MAGIC   "Database" -> "[DATABASE]",
# MAGIC   "PreferredRegions" -> "Central US;East US2;",
# MAGIC   "Collection" -> "[COLLECTION]",
# MAGIC   "SamplingRatio" -> "1.0",
# MAGIC   "query_custom" -> query
# MAGIC ))
# MAGIC 
# MAGIC // Connect via azure-cosmosdb-spark to create Spark DataFrame
# MAGIC val df = spark.read.cosmosDB(readConfig)
# MAGIC df.count
# MAGIC 
# MAGIC // Saving data to Azure Cosmos DB
# MAGIC // Configure connection to the sink collection
# MAGIC val writeConfig = Config(Map(
# MAGIC   "Endpoint" -> "https://[ACCOUNT].documents.azure.com:443/",
# MAGIC   "Masterkey" -> "[MASTER KEY]",
# MAGIC   "Database" -> "[DATABASE]",
# MAGIC   "PreferredRegions" -> "Central US;East US2;",
# MAGIC   "Collection" -> "[COLLECTION]",
# MAGIC   "WritingBatchSize" -> "100"
# MAGIC ))
# MAGIC 
# MAGIC // Upsert the DataFrame to Azure Cosmos DB
# MAGIC import org.apache.spark.sql.SaveMode
# MAGIC df.write.mode(SaveMode.Overwrite).cosmosDB(writeConfig)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cosmos DB Python

# COMMAND ----------

# In Python
# Loading data from Azure Cosmos DB
# Read configuration
query = "SELECT c.colA, c.coln FROM c WHERE c.origin = 'SEA'"
readConfig = {
  "Endpoint" : "https://[ACCOUNT].documents.azure.com:443/", 
  "Masterkey" : "[MASTER KEY]",
  "Database" : "[DATABASE]",
  "preferredRegions" : "Central US;East US2",
  "Collection" : "[COLLECTION]",
  "SamplingRatio" : "1.0",
  "schema_samplesize" : "1000",
  "query_pagesize" : "2147483647",
  "query_custom" : query
}

# Connect via azure-cosmosdb-spark to create Spark DataFrame
df = (spark
  .read
  .format("com.microsoft.azure.cosmosdb.spark")
  .options(**readConfig)
  .load())

# Count the number of flights
df.count()

# Saving data to Azure Cosmos DB
# Write configuration
writeConfig = {
 "Endpoint" : "https://[ACCOUNT].documents.azure.com:443/",
 "Masterkey" : "[MASTER KEY]",
 "Database" : "[DATABASE]",
 "Collection" : "[COLLECTION]",
 "Upsert" : "true"
}

# Upsert the DataFrame to Azure Cosmos DB
(df.write
  .format("com.microsoft.azure.cosmosdb.spark")
  .options(**writeConfig)
  .save())
