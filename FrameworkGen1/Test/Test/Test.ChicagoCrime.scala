// Databricks notebook source
// MAGIC %md 
// MAGIC #Load Energy.EnergyProfile#

// COMMAND ----------

import com.gocogroup.metadata._
import java.util.Calendar
import java.text.SimpleDateFormat;
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._

// COMMAND ----------

// MAGIC %md 
// MAGIC ##Configure##

// COMMAND ----------

dbutils.widgets.text("rundate", "", "rundate (yyyy-MM-dd)")
dbutils.widgets.text("pipeline_name", "", "Pipeline Name")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run Id")
dbutils.widgets.text("version_id", "-1", "Schema Version")


// COMMAND ----------


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
        "datalake"
  )

// COMMAND ----------

displayHTML(pipeline.source.toHtml())

// COMMAND ----------

displayHTML(pipeline.destination.toHtml())

// COMMAND ----------

// MAGIC %md 
// MAGIC ##Extract Transform##

// COMMAND ----------

var keyseed = pipeline.getKeySeed[Long]("Key", Some("PartitionDate"))

// COMMAND ----------

val filterSrc = pipeline.source.getPartitionFilter(addWhere=false).get

// COMMAND ----------

val dfraw = spark.sql(s"""
  SELECT
    ${keyseed} + cast(row_number() over (order by 1) as bigint) as Key,
    `RawKey`,
    -1 as PartitionDate,
    now() as `LoadDate`,
    `FileDate`,
    `SourcePipelineRunId`,
    '${pipeline.pipelineRunId}' AS PipelineRunId,
    AgentUsername,
    AmbassadorId,
    AmendedFromEnergyProfileId,
    BankPaymentDay,
    CreatedAt,
    CurrentSupplierId1,
    CurrentSupplierName1,
    CurrentSupplierType1,
    CurrentSupplierId2,
    CurrentSupplierName2,
    CurrentSupplierType2,
    CustomerId,
    DualFuelBillingTypeId,
    DualFuelIsEconomy7,
    DualFuelPaymentTypeId,
    DualFuelSupplierId,
    DualFuelTariffEndDate,
    DualFuelTariffId,
    ElectricityBillingTypeId,
    ElectricityEstimatedEconomy7DayValue,
    ElectricityEstimatedEconomy7NightValue,
    ElectricityEstimatedUsageId,
    ElectricityEstimatedUsageValue,
    ElectricityIsEconomy7,
    ElectricityPaymentTypeId,
    ElectricitySpend,
    ElectricitySpendFrequencyTypeId,
    ElectricitySpendTypeId,
    ElectricitySupplierId,
    ElectricityTariffEndDate,
    ElectricityTariffId,
    ElectricityUsageEconomy7DaykWh,
    ElectricityUsageEconomy7NightkWh,
    ElectricityUsageTypeId,
    ElectricityUsagekWh,
    EmailAddress,
    EnergyProfileId,
    EnergyTypeId,
    GasBillingTypeId,
    GasEstimatedUsageId,
    GasEstimatedUsageValue,
    GasPaymentTypeId,
    GasSpend,
    GasSpendFrequencyTypeId,
    GasSpendTypeId,
    GasSupplierId,
    GasTariffEndDate,
    GasTariffId,
    GasUsageTypeId,
    GasUsagekWh,
    HasSmartMeter,
    IsDualFuel,
    LastModified,
    MediaCode,
    Mpan,
    Mprn,
    OverrideEnergyTypeId,
    OverridePaymentTypeId,
    SessionId,
    Source,
    SupplyCounty,
    SupplyDoubleLocality,
    SupplyFlatNo,
    SupplyHouseName,
    SupplyHouseNumber,
    SupplyLocality,
    SupplyMpan,
    SupplyMprn,
    SupplyOrganisation,
    SupplyPostcode,
    SupplyRegionId,
    SupplyStreet,
    SupplyThoroughfare,
    SupplyTown,
    WebAnalyticsId
  FROM ${pipeline.source.getObject()} src
  WHERE ${filterSrc}
""")


// COMMAND ----------

val validatedDf = pipeline.source.validateRead(dfraw)

// COMMAND ----------

// MAGIC %md 
// MAGIC ##Load##

// COMMAND ----------

//not corrently partitioned. If Partitioned chucking in the partition column and format will siginificanyl improve performance of the delete

pipeline.deleteExistingPartition(
//   ,Some("PartitionDate"),
//   PartitionKeyFormat.yyyy
)

// COMMAND ----------

//delta insert
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
