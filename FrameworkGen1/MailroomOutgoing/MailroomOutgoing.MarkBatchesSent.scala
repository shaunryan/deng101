// Databricks notebook source
// MAGIC %run ../Includes/SetupEnvironment

// COMMAND ----------

ConnectToDataLake()

// COMMAND ----------

dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run Id")

// COMMAND ----------


val ppipeline_run_id = dbutils.widgets.get("pipeline_run_id")


// COMMAND ----------

val updateSent = s"""
UPDATE savingsengine.weflip_ilikelysavingsopportunityfound
SET IsSent = true, SentDate=now()
WHERE UUID IN (SELECT UUID FROM mailroomoutgoing.weflip_ilikelysavingsopportunityfound WHERE pipeline_run_id = '${ppipeline_run_id}')
"""
spark.sql(updateSent)


// COMMAND ----------

dbutils.notebook.exit(s"MailroomOutgoing.MarkBatchesSent : success")
