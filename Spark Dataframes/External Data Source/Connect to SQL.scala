// Databricks notebook source
val params = Seq("loaddate=afnaf", "34345=ertesrt")
Seq

// COMMAND ----------

import java.util.Properties

case class NotebookExecution(notebook: String, parameter: String)

def log_notebook_execution(notebook:String, parameters:Seq[String]) : Unit =
{
  val notebook_execution_log_table = "logging.notebook_execution"
  val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  val jdbcHostname = dbutils.secrets.get(scope = "dataengineering", key = "decdbhost")
  val jdbcDatabase = dbutils.secrets.get(scope = "dataengineering", key = "decdb")
  val jdbcPassword = dbutils.secrets.get(scope = "dataengineering", key = "decdbpassword")
  val jdbcUsername = dbutils.secrets.get(scope = "dataengineering", key = "decdbuser")

  // Create the JDBC URL without passing in the user and password parameters.
  val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname};database=${jdbcDatabase}"
  
  // Create a Properties() object to hold the parameters.
  val connectionProperties = new Properties()
  connectionProperties.put("password", jdbcPassword)
  connectionProperties.put("user", jdbcUsername)
  connectionProperties.setProperty("Driver", driverClass)

val notebook_execution = new NotebookExecution(notebook, parameters.mkString(", "))
val notebook_execution_df = Seq(notebook_execution).toDF()

  notebook_execution_df.write
       .mode(SaveMode.Append)
       .jdbc(jdbcUrl, notebook_execution_log_table, connectionProperties)
}

val params = Seq("loaddate=afnaf", "34345=ertesrt")
val notebook = dbutils.notebook.getContext().notebookPath.get
log_notebook_execution(notebook, params)


// COMMAND ----------

import java.util.Properties

val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
val jdbcHostname = "gc-grpdata.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "DataEngineeringControl"
val jdbcPassword = "d4t4bricks!"
val jdbcUsername = "databricks"

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname};database=${jdbcDatabase}"
// Create a Properties() object to hold the parameters.

val connectionProperties = new Properties()
connectionProperties.put("password", jdbcPassword) 
connectionProperties.put("user", jdbcUsername)
connectionProperties.setProperty("Driver", driverClass)

//val data_object = spark.read.jdbc(jdbcUrl, "metadata.data_object", connectionProperties)


//val sql = "(select * from myschema.mytable) t1"
//val df = spark.read 
//    .format("jdbc") 
//    .option("driver", driverClass) 
//    .option("url", jdbcUrl) 
//    .option("user", jdbcUsername) 
//    .option("password", jdbcPassword) 
//    .option("dbtable", sql)  
//    .load()
//
//display(df)
val notebook = dbutils.notebook.getContext().notebookPath.get

case class NotebookExecution(notebook: String, parameter: String)

val notebook_execution = new NotebookExecution(notebook, "test")
val notebook_execution_df = Seq(notebook_execution).toDF()

display(notebook_execution_df)

notebook_execution_df.write
     .mode(SaveMode.Append)
     .jdbc(jdbcUrl, "logging.notebook_execution", connectionProperties)



// COMMAND ----------

//create table
spark.table("diamonds").withColumnRenamed("table", "table_number")
     .write
     .jdbc(jdbcUrl, "diamonds", connectionProperties)

//append table
import org.apache.spark.sql.SaveMode

spark.sql("select * from diamonds limit 10").withColumnRenamed("table", "table_number")
     .write
     .mode(SaveMode.Append) // <--- Append to the existing table
     .jdbc(jdbcUrl, "diamonds", connectionProperties)
//overwrite table


spark.table("diamonds").withColumnRenamed("table", "table_number")
     .write
     .mode(SaveMode.Overwrite) // <--- Overwrite the existing table
     .jdbc(jdbcUrl, "diamonds", connectionProperties)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE jdbcTable
// MAGIC USING org.apache.spark.sql.jdbc
// MAGIC OPTIONS (
// MAGIC   url "jdbc:sqlserver://gc-grpdata.database.windows.net;database=DataEngineeringControl",
// MAGIC   dbtable "metadata.data_object",
// MAGIC   user "databricks",
// MAGIC   password "d4t4bricks!"
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from jdbcTable
