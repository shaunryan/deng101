# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # PostgreSQL
# MAGIC 
# MAGIC [Driver](https://mvnrepository.com/artifact/org.postgresql/postgresql)
# MAGIC ```
# MAGIC bin/spark-shell --jars postgresql-42.2.6.jar
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### PostgreSQL Scala Read

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // option 1: using the load method
# MAGIC val jdbcDF1 = spark.read
# MAGIC   .format("jdbc")
# MAGIC   .option("url", "jdbc:postgresql:[DBSERVER]")
# MAGIC   .option("dbtable", "[SCHEMA].[TABLENAME]")
# MAGIC   .option("user", "[USERNAME]")
# MAGIC   .option("password", "[PASSWORD]")
# MAGIC   .load()
# MAGIC 
# MAGIC // option 2: using the JDBC method
# MAGIC // create the connection properties
# MAGIC import java.util.Properties
# MAGIC val cxnProp = new Properties()
# MAGIC cxnProp.put("user", "[USERNAME]")
# MAGIC cxnProp.put("password", "[PASSWORD]")
# MAGIC 
# MAGIC // Load data using the connection properties
# MAGIC val jdbcDF2 = spark.read
# MAGIC   .jdbc("jdbc:postgresql:[DBSERVER]", "[SCHEMA].[TABLENAME]", cxnProp)

# COMMAND ----------

# MAGIC %md
# MAGIC #### PostgreSQL Scala Write

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // option 1: using the save method
# MAGIC val jdbcDF1 = spark.write
# MAGIC   .format("jdbc")
# MAGIC   .option("url", "jdbc:postgresql:[DBSERVER]")
# MAGIC   .option("dbtable", "[SCHEMA].[TABLENAME]")
# MAGIC   .option("user", "[USERNAME]")
# MAGIC   .option("password", "[PASSWORD]")
# MAGIC   .save()
# MAGIC 
# MAGIC 
# MAGIC // option 2: using the JDBC method
# MAGIC // create the connection properties
# MAGIC import java.util.Properties
# MAGIC val cxnProp = new Properties()
# MAGIC cxnProp.put("user", "[USERNAME]")
# MAGIC cxnProp.put("password", "[PASSWORD]")
# MAGIC 
# MAGIC // Save data using the connection properties
# MAGIC val jdbcDF2 = spark.write
# MAGIC   .jdbc("jdbc:postgresql:[DBSERVER]", "[SCHEMA].[TABLENAME]", cxnProp)

# COMMAND ----------

# MAGIC %md
# MAGIC #### PostgreSQL Python Read

# COMMAND ----------

# In Python
# Read Option 1: Loading data from a JDBC source using load method
jdbcDF1 = (spark
  .read
  .format("jdbc") 
  .option("url", "jdbc:postgresql://[DBSERVER]")
  .option("dbtable", "[SCHEMA].[TABLENAME]")
  .option("user", "[USERNAME]")
  .option("password", "[PASSWORD]")
  .load())

# Read Option 2: Loading data from a JDBC source using jdbc method
jdbcDF2 = (spark
  .read 
  .jdbc("jdbc:postgresql://[DBSERVER]", "[SCHEMA].[TABLENAME]",
          properties={"user": "[USERNAME]", "password": "[PASSWORD]"}))

# COMMAND ----------

# MAGIC %md
# MAGIC #### PostgreSQL Python Write

# COMMAND ----------



# Write Option 1: Saving data to a JDBC source using save method
(jdbcDF1
  .write
  .format("jdbc")
  .option("url", "jdbc:postgresql://[DBSERVER]")
  .option("dbtable", "[SCHEMA].[TABLENAME]") 
  .option("user", "[USERNAME]")
  .option("password", "[PASSWORD]")
  .save())

# Write Option 2: Saving data to a JDBC source using jdbc method
(jdbcDF2
  .write 
  .jdbc("jdbc:postgresql:[DBSERVER]", "[SCHEMA].[TABLENAME]",
          properties={"user": "[USERNAME]", "password": "[PASSWORD]"}))
