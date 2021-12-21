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
val metadata = new DatabrickPipelineMetadata(prundate, ppipeline_name, ppipeline_run_id, notebook, "-1", "stageOutgoing")
val loaddateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val loaddate = loaddateformat.format(Calendar.getInstance().getTime())
metadata.showHtml

// COMMAND ----------


val root = getDataLakeStorageAccount
val dir = "raw/mailroomoutgoing/"
val tableName = metadata.destinationObject.toLowerCase()
val path = s"${root}${dir}${tableName}"

// COMMAND ----------

val sqlDropTable = s"""
  DROP TABLE IF EXISTS ${tableName}
"""
spark.sql(sqlDropTable)

//shouldn't need to do this but we do for some reasons
dbutils.fs.rm(path, true)

val sqlCreateTable = s"""
  CREATE TABLE ${tableName} (
    `uuid` STRING,
    `data` STRING,
    `type` STRING,
    `time` TIMESTAMP,
    `pipeline_run_id` STRING,
    `is_test` BOOLEAN,
    `load_date` TIMESTAMP
  )
  USING delta
  OPTIONS (
    path \"${path}\"
  )
"""
spark.sql(sqlCreateTable)


// COMMAND ----------

val dfraw = spark.sql(s"""
  SELECT 
    UUID,
    SubscriberKey,
    LoadDate as Time,
    to_json(Struct(
      AffiliateID,
      SubscriberKey,
      CustomerId,
      ifnull(EncryptedCustomerId, "") as EncryptedCustomerId,
      SignupId,
      Timestamp,
      ExistingTariffCode,
      SelectedTariffCode,
      ExistingPriceSearchString,
      SelectedPriceSearchString,
      ConfidenceInterval,
      CurrentAnnualCosts,
      Savings,
      SelectedAnnualCosts,
      FuelType,
      IsSavingsOpportunityProcessingEnabled,
      ConsumptionFigures
    )) Data,
    'GoCompare.Contracts.Weflip.Energy.v1.ILikelySavingsOpportunityFound' Type,
    IsTest
  FROM ${metadata.sourceObject}
  WHERE IsSent = false
  AND Exclude = false or Exclude is null
  """)

val dfValidated = metadata.HandleReadExceptions(dfraw)

val dfToGo = dfValidated     
          .withColumn("LoadDate", lit(loaddate))
          .withColumn("PipelineRunId", lit(metadata.lpipelineRunId))
          .withColumn("IsValid", expr("if(length(ifnull(ColSchemaCheck,''))>0, false, true)"))
          .withColumn("IsTest", expr("if(instr(lcase(SubscriberKey), '-test') > 0, true, false)"))
          .filter("IsValid = true")
          .select(
            $"UUID".as("uuid"),
            $"Data".as("data"),
            $"Type".as("type"),
            $"Time".as("time"),
            $"PipelineRunId".as("pipeline_run_id"),
            $"IsTest".as("is_test"),
            $"LoadDate".as("load_date")
            )

//write the contract validated data into the target
dfToGo.write.option("badRecordsPath", metadata.writeExceptionPath).insertInto(metadata.destinationObject)

//write the validation exceptions
val writeRowCount = spark.sql(s"""select count(*) from ${metadata.destinationObject} WHERE pipeline_run_id = '${metadata.lpipelineRunId}' """).first.getLong(0)
metadata.HandleWriteExceptions(writeRowCount)



// COMMAND ----------

DataEngineeringControl.showNotebookExecutionReport(metadata.lpipelineRunId, notebook)

// COMMAND ----------

dbutils.notebook.exit(s"${notebook} : success")
