# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # SQLServer
# MAGIC 
# MAGIC [Driver](https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=sql-server-2017)
# MAGIC ```
# MAGIC bin/spark-shell --jars mssql-jdbc-7.2.2.jre8.jar
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL Server Scala

# COMMAND ----------

# MAGIC %scala
# MAGIC // In Scala
# MAGIC // Loading data from a JDBC source
# MAGIC // Configure jdbcUrl
# MAGIC val jdbcUrl = "jdbc:sqlserver://[DBSERVER]:1433;database=[DATABASE]"
# MAGIC 
# MAGIC // Create a Properties() object to hold the parameters. 
# MAGIC // Note, you can create the JDBC URL without passing in the
# MAGIC // user/password parameters directly.
# MAGIC val cxnProp = new Properties()
# MAGIC cxnProp.put("user", "[USERNAME]") 
# MAGIC cxnProp.put("password", "[PASSWORD]") 
# MAGIC cxnProp.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
# MAGIC 
# MAGIC // Load data using the connection properties
# MAGIC val jdbcDF = spark.read.jdbc(jdbcUrl, "[TABLENAME]", cxnProp)
# MAGIC 
# MAGIC // Saving data to a JDBC source
# MAGIC jdbcDF.write.jdbc(jdbcUrl, "[TABLENAME]", cxnProp)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL Server Python

# COMMAND ----------

# In Python
# Configure jdbcUrl
jdbcUrl = "jdbc:sqlserver://[DBSERVER]:1433;database=[DATABASE]"

# Loading data from a JDBC source
jdbcDF = (spark
  .read
  .format("jdbc") 
  .option("url", jdbcUrl)
  .option("dbtable", "[TABLENAME]")
  .option("user", "[USERNAME]")
  .option("password", "[PASSWORD]")
  .load())

# Saving data to a JDBC source
(jdbcDF
  .write
  .format("jdbc") 
  .option("url", jdbcUrl)
  .option("dbtable", "[TABLENAME]")
  .option("user", "[USERNAME]")
  .option("password", "[PASSWORD]")
  .save())
