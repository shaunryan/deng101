# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # MySQL
# MAGIC 
# MAGIC [Driver](https://dev.mysql.com/downloads/connector/j/)
# MAGIC ```
# MAGIC bin/spark-shell --jars mysql-connector-java_8.0.16-bin.jar
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### MySQL Scala

# COMMAND ----------

# MAGIC %scala
# MAGIC // In Scala
# MAGIC // Loading data from a JDBC source using load 
# MAGIC val jdbcDF = spark
# MAGIC   .read
# MAGIC   .format("jdbc")
# MAGIC   .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
# MAGIC   .option("driver", "com.mysql.jdbc.Driver")
# MAGIC   .option("dbtable", "[TABLENAME]")
# MAGIC   .option("user", "[USERNAME]")
# MAGIC   .option("password", "[PASSWORD]")
# MAGIC   .load()
# MAGIC 
# MAGIC // Saving data to a JDBC source using save 
# MAGIC jdbcDF
# MAGIC   .write
# MAGIC   .format("jdbc")
# MAGIC   .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
# MAGIC   .option("driver", "com.mysql.jdbc.Driver")
# MAGIC   .option("dbtable", "[TABLENAME]")
# MAGIC   .option("user", "[USERNAME]")
# MAGIC   .option("password", "[PASSWORD]")
# MAGIC   .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### PostgreSQL Python

# COMMAND ----------

# In Python
# Loading data from a JDBC source using load 
jdbcDF = (spark
  .read
  .format("jdbc")
  .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
  .option("driver", "com.mysql.jdbc.Driver") 
  .option("dbtable", "[TABLENAME]")
  .option("user", "[USERNAME]")
  .option("password", "[PASSWORD]")
  .load())

# Saving data to a JDBC source using save 
(jdbcDF
  .write 
  .format("jdbc") 
  .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
  .option("driver", "com.mysql.jdbc.Driver") 
  .option("dbtable", "[TABLENAME]") 
  .option("user", "[USERNAME]")
  .option("password", "[PASSWORD]")
  .save())
