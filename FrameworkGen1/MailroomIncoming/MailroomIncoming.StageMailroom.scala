// Databricks notebook source
// MAGIC %run AzureDataFactory/Includes/DataEngineeringControl

// COMMAND ----------

ConnectToDataLake()

// COMMAND ----------

// MAGIC %md 
// MAGIC ##Configure##

// COMMAND ----------

dbutils.widgets.text("rundate", "", "rundate (yyyy-MM-dd)")
dbutils.widgets.text("pipeline_name", "", "Pipeline Name")
dbutils.widgets.text("transformation", "", "Transformation Name")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run Id")
dbutils.widgets.text("version_id", "-1", "Schema Version")

// COMMAND ----------

import java.util.Calendar
import java.text.SimpleDateFormat;
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._

/*
  pull out the date - this will be a string so could be anything
  therefore parse it to a date to ensure that it's a valid date
*/
val prundate = dbutils.widgets.get("rundate")
val ppipeline_name = dbutils.widgets.get("pipeline_name")
val ptransformation = dbutils.widgets.get("transformation")
val ppipeline_run_id = dbutils.widgets.get("pipeline_run_id")
val pversion_id = dbutils.widgets.get("version_id")

/* log the notebook execution for troubleshooting production issues.*/
val notebook = ptransformation
println("ptransformation ===>>> "+ptransformation)

/*
  Data pre-preparation
*/
val metadata = new DatabrickPipelineMetadata(prundate, ppipeline_name, ppipeline_run_id, notebook, pversion_id)
val loaddateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val loaddate = loaddateformat.format(Calendar.getInstance().getTime())
spark.sql("""drop table if exists  """ + metadata.destinationObjectTemp)
metadata.showHtml


// COMMAND ----------

// MAGIC %md 
// MAGIC ##Extract, Transform & Load##
// MAGIC 
// MAGIC Has to be in 1 cell so we can check the path exists. It's conceivable that it might not if the load frequency is reduced on a realtime feed
// MAGIC i.e. the smaller the window gets the more likely there may not be any events to recieve.

// COMMAND ----------

if (metadata.dataRootPathExists) // only proceed if there is something to load
{
  //delete data for rundate being loaded... 
  DataPrePrep.DeletePartition(metadata.destinationObject, prundate)
  
  val windowSpecDedup = Window.partitionBy(col("uuid")).orderBy(col("load_date").desc)
  //EXTRACT
  
  val df = metadata.loadDataFrame()
  
  val dfContract =   df
     .withColumn("FileDate", expr("to_timestamp(left(concat(substring_index(substring_index(input_file_name(), '_', -1),'.', 1), '000000'), 8), 'yyyyMMdd')"))
    .withColumn("FilePipelineRunId", expr("substring_index(substring_index(input_file_name(), '_', -3), '_', 1)")) 
    .withColumn("Receiver", expr("substring_index(substring_index(input_file_name(), '_', 1), '/', -1)")) 
    .withColumn("SchemaVersion", expr("cast(substring_index(substring_index(input_file_name(), '_', -2), '_', 1) as int)")) 
    .withColumn("FilterContract", lit(ptransformation.toLowerCase))
    .withColumn("IsFilterContract", expr(s"if(instr(lower(type),concat('${ptransformation.toLowerCase}',';')) > 0 , true, false)"))
    .where("IsFilterContract = true")
    .drop("IsFilterContract")
  
  //this handles spark exception rows and validates the json using the schema 4 contract definitions
  //json validation result is added in a new string column called ColSchemaCheck, if it's null or empty then its ok.
  //NOTE: in order to validate before the final transformation we incurr the cost of read (an action is called) - in order to validate the data it's worth it
  val dfValidated = metadata.HandleReadExceptions(dfContract)
  //if you get index out of bounds error then it's because the exception files need deleting from \dataegineering\exceptions\ingestmailroom
  

  //TRANSFORM - we're not shredding it on the initial stage. Just store as string.

  val dfAlreadyLoaded = spark.sql(s"""SELECT UUID as TUUID FROM ${metadata.destinationObject}""")

  val dfraw = dfValidated
    .withColumn("Dedupe", row_number().over(windowSpecDedup)).filter($"Dedupe" === 1).drop($"Dedupe")
    .withColumn("LoadDate", lit(loaddate))
    .withColumn("IsValid", expr("if(length(ifnull(ColSchemaCheck,''))>0, false, true)"))
    .join(dfAlreadyLoaded, (dfValidated.col("UUID") === dfAlreadyLoaded("TUUID")), "left_outer")
    .where("TUUID IS NULL")
    .select($"uuid".as("UUID"), $"id".as("Id"), $"data".as("Data"), $"type".as("Type"), $"time".as("Time"), $"FilterContract".as("TransformName"), $"pipeline_run_id".as("PipelineRunId"), $"load_date".as("MailroomBatchLoadDate"),$"Receiver",$"FilePipelineRunId",$"SchemaVersion",$"FileDate",$"LoadDate",$"ColSchemaCheck".as("InvalidMessage"),$"IsValid")
  
  //LOAD
  dfraw.write.option("badRecordsPath", metadata.writeExceptionPath).insertInto(metadata.destinationObject) 
  //metadata.destinationObject = MailroomIncoming.ISwitchCompleted
  //stores in DataBricks - 'Workspace/DatabaseProjects/MailroomIncoming/Full DESTRUCTIVE/tables/ISwitchStarted'
  
  val writeRowCount = spark.sql(s"""select count(*) from ${metadata.destinationObject} where LoadDate = "${loaddate}" """).first.getLong(0)
  
  metadata.HandleWriteExceptions(writeRowCount)
}
else
{
  metadata.LogNotebookExecutionNoData()
}

// COMMAND ----------

// MAGIC %md 
// MAGIC ##Show Me The Numbers!##

// COMMAND ----------

DataEngineeringControl.showNotebookExecutionReport(ppipeline_run_id, notebook)

// COMMAND ----------

dbutils.notebook.exit(s"${notebook} #${dbutils.notebook.getContext.currentRunId} : success")
