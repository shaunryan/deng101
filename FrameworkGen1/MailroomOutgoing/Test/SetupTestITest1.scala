// Databricks notebook source
// MAGIC %run AzureDataFactory/Includes/SetupEnvironment

// COMMAND ----------

ConnectToDataLake()

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types._

val schema = new StructType()
  .add($"ContainsEstimates".boolean)
  .add($"GasUsagekWh".double)
  .add($"ElectricityUsagekWh".double)
  .add($"NightUsePercent".double)
  .add($"MeterTypeElec".string)



val testData = sqlContext.read.format("csv")
  .option("header", "true")
  .option("quote", "\"")
  .option("escape", "\"")
  .load("adl://gcdatabricksdlsdev.azuredatalakestore.net/raw/mailroomoutgoing/testdata/iLikelySavingsOpportunityFound_ITest1.csv")
  .withColumn("ConsumptionFiguresStruc", from_json(col("ConsumptionFigures"), schema))
  .filter("Test = 'Load1'")
  .select(
    $"UUID",
    $"AffiliateID",
    $"SubscriberKey",
    $"CustomerId",
    $"EncryptedCustomerId",
    $"SignupId",
    $"Timestamp",
    $"ExistingTariffCode",
    $"SelectedTariffCode",
    $"ExistingPriceSearchString",
    $"SelectedPriceSearchString",
    $"ConfidenceInterval",
    $"CurrentAnnualCosts",
    $"Savings",
    $"SelectedAnnualCosts",
    $"FuelType",
    $"IsSavingsOpportunityProcessingEnabled",
    $"ConsumptionFiguresStruc".as("ConsumptionFigures"),
    $"SwitchType",
    $"ExcludeReason",
    $"Exclude",
    $"PipelineRunId",
    $"LoadDate",
    $"IsSent",
    $"IsTest",
    $"SentDate"
    )

testData.write.insertInto("SavingsEngine.weflip_ilikelysavingsopportunityfound")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from SavingsEngine.weflip_ilikelysavingsopportunityfound where IsSent = false --where LoadDate = '1900-01-01T00:00:00.000+0000'

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select count(1) from SavingsEngine.weflip_ilikelysavingsopportunityfound where IsSent = false

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select count(1) from SavingsEngine.weflip_ilikelysavingsopportunityfound where IsSent = true

// COMMAND ----------

// MAGIC %sql
// MAGIC --delete from SavingsEngine.weflip_ilikelysavingsopportunityfound
