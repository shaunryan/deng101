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
            $"shredded.ExistingSignupId".as("ExistingSignupId"),
            $"shredded.NewSignupId".as("NewSignupId"),
            $"shredded.QuoteId".as("QuoteId"),
            $"shredded.IsAutoSwitch".as("IsAutoSwitch"),
            $"shredded.FuelType".as("FuelType"),
            $"shredded.TariffCode".as("TariffCode"),
            $"shredded.ExportFileName".as("ExportFileName"),
            $"shredded.TopSavings".as("TopSavings"),
            $"shredded.TopBill".as("TopBill"),
            $"shredded.ExistingSpend".as("ExistingSpend"),
            $"shredded.NewSpend".as("NewSpend"),
            $"shredded.NewSavings".as("NewSavings"),
            $"shredded.ThirdPartyPaymentTypeId".as("ThirdPartyPaymentTypeId"),
            $"shredded.SaleStartDate".as("SaleStartDate"),
            $"shredded.SaleEndDate".as("SaleEndDate"),
            $"shredded.IsMarketingEmailConsentGranted".as("IsMarketingEmailConsentGranted"),
            $"shredded.PriceId".as("PriceId"),
            $"shredded.IsTariffFixed".as("IsTariffFixed"),
            $"shredded.FixedTermEndDate".as("FixedTermEndDate"),
            $"shredded.ExitFee".as("ExitFee"),
            $"shredded.SingleOrSeparateSwitch".as("SingleOrSeparateSwitch"),
            $"shredded.RMRCompliantCalculation".as("RMRCompliantCalculation"),
            $"shredded.FilterPaperlessBilling".as("FilterPaperlessBilling"),
            $"shredded.ExistingkWhsIntervalElec".as("ExistingkWhsIntervalElec"),
            $"shredded.ExistingkWhsIntervalGas".as("ExistingkWhsIntervalGas"),
            $"shredded.SupplierLogo".as("SupplierLogo"),
            
            $"shredded.ConsumptionForCalculation.ContainsEstimates".as("CFCContainsEstimates"),
            $"shredded.ConsumptionForCalculation.GasUsagekWh".as("CFCGasUsagekWh"),
            $"shredded.ConsumptionForCalculation.ElectricityUsagekWh".as("CFCElectricityUsagekWh"),
            $"shredded.ConsumptionForCalculation.NightUsePercent".as("CFCNightUsePercent"),
            $"shredded.ConsumptionForCalculation.MeterTypeElec".as("CFCMeterTypeElec"),
            
            $"shredded.CustomerData.BillingAddressData.County".as("BillingAddressCounty"),
            $"shredded.CustomerData.BillingAddressData.DoubleLocality".as("BillingAddressDoubleLocality"),
            $"shredded.CustomerData.BillingAddressData.FlatNo".as("BillingAddressFlatNo"),
            $"shredded.CustomerData.BillingAddressData.HouseName".as("BillingAddressHouseName"),
            $"shredded.CustomerData.BillingAddressData.HouseNumber".as("BillingAddressHouseNumber"),
            $"shredded.CustomerData.BillingAddressData.Locality".as("BillingAddressLocality"),
            $"shredded.CustomerData.BillingAddressData.Organisation".as("BillingAddressOrganisation"),
            $"shredded.CustomerData.BillingAddressData.Postcode".as("BillingAddressPostcode"),
            $"shredded.CustomerData.BillingAddressData.RegionId".as("BillingAddressRegionId"),
            $"shredded.CustomerData.BillingAddressData.Street".as("BillingAddressStreet"),
            $"shredded.CustomerData.BillingAddressData.Thoroughfare".as("BillingAddressThoroughfare"),
            $"shredded.CustomerData.BillingAddressData.Town".as("BillingAddressTown"),

            $"shredded.CustomerData.MpanData.MeterSerialElec".as("MpanMeterSerialElec"),
            $"shredded.CustomerData.MpanData.MpanLower".as("MpanLower"),
            $"shredded.CustomerData.MpanData.MpanTopLine".as("MpanTopLine"),
            $"shredded.CustomerData.MpanData.Source".as("MpanSource"),
            
            $"shredded.CustomerData.MprnData.MeterSerialGas".as("MprnMeterSerialGas"),
            $"shredded.CustomerData.MprnData.Mprn".as("Mprn"),
            $"shredded.CustomerData.MprnData.Source".as("MprnSource"),

            $"shredded.CustomerData.MobileNumber".as("MobileNumber"),
            $"shredded.CustomerData.TitleId".as("TitleId"),
            $"shredded.CustomerData.FirstName".as("FirstName"),
            $"shredded.CustomerData.Surname".as("Surname"),
            $"shredded.CustomerData.EmailAddress".as("EmailAddress"),
            $"shredded.CustomerData.TelephoneNumber".as("TelephoneNumber"),
            $"shredded.CustomerData.DateOfBirth".as("DateOfBirth"),
            
            $"shredded.SupplyAddress.County".as("SupplyAddressCounty"),
            $"shredded.SupplyAddress.DoubleLocality".as("SupplyAddressDoubleLocality"),
            $"shredded.SupplyAddress.FlatNo".as("SupplyAddressFlatNo"),
            $"shredded.SupplyAddress.HouseName".as("SupplyAddressHouseName"),
            $"shredded.SupplyAddress.HouseNumber".as("SupplyAddressHouseNumber"),
            $"shredded.SupplyAddress.Locality".as("SupplyAddressLocality"),
            $"shredded.SupplyAddress.Organisation".as("SupplyAddressOrganisation"),
            $"shredded.SupplyAddress.Postcode".as("SupplyAddressPostcode"),
            $"shredded.SupplyAddress.RegionId".as("SupplyAddressRegionId"),
            $"shredded.SupplyAddress.Street".as("SupplyAddressStreet"),
            $"shredded.SupplyAddress.Thoroughfare".as("SupplyAddressThoroughfare"),
            $"shredded.SupplyAddress.Town".as("SupplyAddressTown"),

            $"shredded.SelectedPriceSearchString".as("SelectedPriceSearchString"),
            $"shredded.ExistingGasPriceSearchString".as("ExistingGasPriceSearchString"),
            $"shredded.ExistingElecPriceSearchString".as("ExistingElecPriceSearchString"),
            $"shredded.ExistingGasTariffCode".as("ExistingGasTariffCode"),
            $"shredded.ExistingElecTariffCode".as("ExistingElecTariffCode"),
            $"shredded.ExistingSupplierGas".as("ExistingSupplierGas"),
            $"shredded.ExistingSupplierElec".as("ExistingSupplierElectric"),
            $"shredded.SupplierName".as("SupplierName"),
            $"shredded.SupplierTariffName".as("SupplierTariffName"),
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

// COMMAND ----------


