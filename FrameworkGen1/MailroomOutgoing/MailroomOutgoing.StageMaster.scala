// Databricks notebook source
// MAGIC %md
// MAGIC ## Dependancy Imports

// COMMAND ----------

import java.util.UUID.randomUUID

// COMMAND ----------

// MAGIC %run AzureDataFactory/Includes/Workflow

// COMMAND ----------

ConnectToDataLake()

// COMMAND ----------

// MAGIC %md
// MAGIC ##Setup Parameters

// COMMAND ----------

dbutils.widgets.text("rundate", "", "rundate (yyyy-MM-dd)")
dbutils.widgets.text("pipeline_name", "", "Pipeline Name")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run Id")
dbutils.widgets.text("version_id", "-1", "Schema Version")
dbutils.widgets.text("send_alert_to", "databricks.alerts@gocompare.com", "Send Alert To")
dbutils.widgets.text("pipeline_batch_id", "", "Pipeline Batch Id")

val prundate = dbutils.widgets.get("rundate")
val ppipeline_name = dbutils.widgets.get("pipeline_name")
val ppipeline_run_id = if (Option(dbutils.widgets.get("pipeline_run_id")).getOrElse("").isEmpty) randomUUID().toString else dbutils.widgets.get("pipeline_run_id")
val pversion_id = dbutils.widgets.get("version_id")
val psend_alert_to = dbutils.widgets.get("send_alert_to")


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##Load Stage

// COMMAND ----------

val wfc = new WorkflowContainer(prundate, ppipeline_name, ppipeline_run_id, "stageOutgoing", pversion_id)
wfc.execute

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##Collate & Send Stage Run Report

// COMMAND ----------

dbutils.notebook.run(
  "/AzureDataFactory/Includes/CollateRunReport",
  600,
  Map("pipeline_name" -> ppipeline_name, "pipeline_run_id" -> ppipeline_run_id, "send_to" -> psend_alert_to)
)
