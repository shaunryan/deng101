// Databricks notebook source
// MAGIC %md
// MAGIC ## Dependancy Imports

// COMMAND ----------

import com.gocogroup.metadata._
import util.{Try,Failure,Success}

// COMMAND ----------

// MAGIC %md
// MAGIC ##Setup Parameters

// COMMAND ----------

dbutils.widgets.text("rundate", "", "rundate (yyyy-MM-dd)")
dbutils.widgets.text("pipeline_name", "", "Pipeline Name")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run Id")
dbutils.widgets.text("version_id", "-1", "Schema Version")
dbutils.widgets.text("send_alert_to", "databricks.alerts@gocompare.com", "Send Alert To")

val rundate = dbutils.widgets.get("rundate")
val pipelineName = dbutils.widgets.get("pipeline_name")
var pipelineRunId = dbutils.widgets.get("pipeline_run_id")
val sendTo = dbutils.widgets.get("send_alert_to")

if (rundate=="*") throw new Exception("Reloadng all the data! Are you sure?")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##Load Delta Raw

// COMMAND ----------

val wfc = new WorkflowContainer(rundate, pipelineRunId, pipelineName, "datalakeraw")
Try{
  pipelineRunId = wfc.execute
} match
{
  case Success(s) =>
  case Failure(f) =>
  {
      //handled just for test. This exception is specifically for halting the load in production due to severe source data issues
      println(s"Workflow pipeline:${pipelineName} layer:datalakeraw pipelineRunId:${wfc._pipelineRunId} rundate:${rundate} container execution failed.\n ${f}")
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##Collate & Send Stage Run Report
// MAGIC 
// MAGIC Currently disabled since we're loading another master process after this one that sends the pipeline alert
// MAGIC It's temporary whilst we're capture switch entity history.

// COMMAND ----------

val rpt = new PipelineReport(wfc._pipelineRunId, pipelineName)
rpt.send(sendTo)

// COMMAND ----------

displayHTML(rpt.getLogReport())
