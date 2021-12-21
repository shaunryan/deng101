// Databricks notebook source
// MAGIC %md 
// MAGIC #Load Raw Chicago Crime
// MAGIC ###Source format errors below threshold

// COMMAND ----------

import com.gocogroup.metadata._

// COMMAND ----------

// MAGIC %md 
// MAGIC ##Configure##

// COMMAND ----------

dbutils.widgets.text("rundate", "", "rundate (yyyy-MM-dd)")
dbutils.widgets.text("pipeline_name", "", "Pipeline Name")
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
val ppipeline_run_id = dbutils.widgets.get("pipeline_run_id")
val pversion_id = dbutils.widgets.get("version_id")
if (prundate=="*") throw new Exception("Reloadng all the data! Are you sure?")

val notebook = dbutils.notebook.getContext().notebookPath.get.split("/").last
val pipeline = MetadataFactory(
        notebook, 
        ppipeline_run_id,
        ppipeline_name,
        prundate,
        "datalakeraw"
  )

// COMMAND ----------

displayHTML(pipeline.source.toHtml())

// COMMAND ----------

displayHTML(pipeline.destination.toHtml())

// COMMAND ----------

// MAGIC %md 
// MAGIC ##Extract##

// COMMAND ----------

val df = pipeline.source.load()
val validatedDf = pipeline.source.validateRead(df)

// COMMAND ----------

// MAGIC %md 
// MAGIC ##Transform##

// COMMAND ----------

var keyseed = pipeline.getKeySeed[Long]("Key", Some("PartitionDate"))

// COMMAND ----------

//create a window function to de-dupe the data.
val windowSpecKey = Window.orderBy(lit(1).asc)
//prepend the standard columns then just absorb the schema coming through in this layer
val columns: Array[String] = Array("Key", "PartitionDate", "LoadDate", "FileDate", "SourcePipelineRunId", "PipelineRunId", "InvalidMessage", "IsValid") ++ df.columns.filter(_!="Filename")

val dfraw = validatedDf
// standard etl columns start
  .withColumn("FileDate", expr("to_timestamp(left(substring_index(substring_index(Filename, '_', -2), '_', 1),8), 'yyyyMMdd')"))
  .withColumn("Key", (lit(keyseed) + row_number().over(windowSpecKey)))
  .withColumn("PartitionDate", expr("year(FileDate)"))
  .withColumn("LoadDate",  expr("now()"))
  .withColumn("SourcePipelineRunId", expr("substring_index(substring_index(Filename, '_', -3), '_', 1)"))
  .withColumn("PipelineRunId", lit(pipeline.destination.pipelineRunId))
  .withColumn("InvalidMessage", expr("CAST(NULL AS String)"))
  .withColumn("IsValid", lit(true))
// standard etl columns end
  .select(columns.head, columns.tail: _*)

// COMMAND ----------

// MAGIC %md
// MAGIC ###Delete Existing
// MAGIC 
// MAGIC 
// MAGIC ####Important Note: __deleteExistingIn__ is a safe delete for the raw layer
// MAGIC   
// MAGIC   It deletes the destination file periods in the source data that we're loading and automatically uses the partitionkey for performance if it can.
// MAGIC   It's safe because the the file landing zone may not (probably won't because of GDPR) have all the data. So if processes with a rundate of * it will only clear and reload the table of data that's available to reload rather than wipe everything out.
// MAGIC   So it allows reload of data that's there but protects deletion of data that's been cleared from the landing layer
// MAGIC   

// COMMAND ----------

//not corrently partitioned. If Partitioned chucking in the partition column and format will siginificanyl improve performance of the delete

pipeline.deleteExistingIn(
  dfraw
//   ,Some("PartitionDate"),
//   PartitionKeyFormat.yyyy
)

// COMMAND ----------

// MAGIC %md 
// MAGIC ##Load##

// COMMAND ----------


dfraw
  .write
  .option("mergeSchema", pipeline.destination.mergeSchema.toString())
  .option("badRecordsPath", pipeline.destination.exceptionPath)
  .format("delta")
  .mode("append")
  .save(pipeline.destination.getDatabasePath())


// COMMAND ----------

pipeline.destination.validateWrite()

// COMMAND ----------

displayHTML(pipeline.getLogReport())

// COMMAND ----------

dbutils.notebook.exit("success")
