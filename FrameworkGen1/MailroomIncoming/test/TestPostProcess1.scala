// Databricks notebook source
// MAGIC %run AzureDataFactory/Includes/SetupEnvironment

// COMMAND ----------

ConnectToDataLake()

// COMMAND ----------

dbutils.widgets.text("testRunId", "", "Test Run Id")

// COMMAND ----------

import java.util.UUID.randomUUID
val ptestRunId = dbutils.widgets.get("testRunId")
val testRunId = if (Option(ptestRunId).getOrElse("").isEmpty) randomUUID().toString else ptestRunId
var environment = getEnvironment

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##Run Tests

// COMMAND ----------

dbutils.notebook.run("/AzureDataFactory/MailroomIncoming/test/RunTests1", 3600, Map("testRunId" -> testRunId, "environment" -> environment))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Collate Result Report

// COMMAND ----------

// MAGIC %run AzureDataFactory/Includes/lib/obj.TestReport

// COMMAND ----------

TestReport.collateTestReport(testRunId, environment)
