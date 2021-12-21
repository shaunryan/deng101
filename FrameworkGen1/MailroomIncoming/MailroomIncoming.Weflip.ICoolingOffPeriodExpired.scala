// Databricks notebook source
// MAGIC %run ../Includes/DataEngineeringControl

// COMMAND ----------

ConnectToDataLake()

// COMMAND ----------

dbutils.widgets.text("rundate", "", "rundate (yyyy-MM-dd)")
dbutils.widgets.text("pipeline_name", "", "Pipeline Name")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run Id")

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
val ppipeline_run_id = dbutils.widgets.get("pipeline_run_id")

/* log the notebook execution for troubleshooting production issues.*/
val notebook = dbutils.notebook.getContext().notebookPath.get.split("/").last

/*
  Data pre-preparation
*/
val metadata = new DatabrickPipelineMetadata(prundate, ppipeline_name, ppipeline_run_id, notebook, "-1", "stageShred")
val loaddateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val loaddate = loaddateformat.format(Calendar.getInstance().getTime())
metadata.showHtml

// COMMAND ----------

val windowSpecKey = Window.orderBy(lit(1).asc)
val keyseed = 0L

val colSchema = metadata.getColumnSchema("data")
val schemaPath = metadata.dataLakeService + "/" + colSchema.ColumnSparkSchemaPath
val sparkschema = DataEngineeringControl.getSparkSchema(schemaPath)

DataPrePrep.DeletePartition(metadata.destinationObject, prundate, "BatchRundate")

val filterSrc = DataPrePrep.GetFilterPartition(prundate, "FileDate")
val dfraw = spark.sql(s"SELECT UUID, Data, FileDate as BatchRunDate FROM ${metadata.sourceObject} ${filterSrc}")

val dfValidated = metadata.HandleReadExceptions(dfraw)

val dfPrep = dfraw  
          .withColumn("shredded", from_json($"data", sparkschema))
          .withColumn("LoadDate", lit(loaddate))
          .withColumn("Key", (lit(keyseed) + row_number().over(windowSpecKey).cast("string")).cast("bigint"))
          .withColumn("PipelineRunId", lit(metadata.lpipelineRunId)) 
          .withColumn("IsTest", expr("if(instr(lcase(shredded.SubscriberKey), '-test') > 0, true, false)"))
          .where(col("IsValid"))
          .select(
            $"UUID",
            $"shredded.CustomerId".as("CustomerId"), 
            $"shredded.AffiliateID".as("AffiliateID"), 
            $"shredded.EncryptedCustomerId".as("EncryptedCustomerId"), 
            $"shredded.SubscriberKey".as("SubscriberKey"), 
            $"shredded.Timestamp".as("Timestamp"),
            $"shredded.StatusFlag".as("StatusFlag"),
            $"shredded.SignupId".as("SignupId"),
            $"shredded.QuoteId".as("QuoteId"),
            $"shredded.IsAutoSwitch".as("IsAutoSwitch"),
            $"LoadDate",
            $"PipelineRunId",
            $"BatchRundate"
            )

dfPrep.write.option("badRecordsPath", metadata.writeExceptionPath).insertInto(metadata.destinationObject)
val filterDst = DataPrePrep.GetFilterPartition(prundate, "BatchRundate")
val writeRowCount = spark.sql(s"""select count(*) from ${metadata.destinationObject} ${filterDst}""").first.getLong(0)

metadata.HandleWriteExceptions(writeRowCount)




// COMMAND ----------

DataEngineeringControl.showNotebookExecutionReport(metadata.lpipelineRunId, notebook)

// COMMAND ----------

dbutils.notebook.exit(s"${notebook} #${dbutils.notebook.getContext.currentRunId} : success")
