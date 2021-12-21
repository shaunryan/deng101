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
            $"shredded.SignupId".as("SignupId"),
            $"shredded.StatusFlag".as("StatusFlag"),
            $"shredded.Postcode".as("Postcode"),
            $"shredded.FreezeAutoswitch".as("FreezeAutoswitch"),
            $"shredded.SwitchCancellationReason".as("SwitchCancellationReason"),
            
            $"shredded.SupplyAddress.FlatNo".as("SupplyAddressFlatNo"),
            $"shredded.SupplyAddress.HouseName".as("SupplyAddressHouseName"),
            $"shredded.SupplyAddress.HouseNumber".as("SupplyAddressHouseNumber"),
            $"shredded.SupplyAddress.Thoroughfare".as("SupplyAddressThoroughfare"),
            $"shredded.SupplyAddress.Street".as("SupplyAddressStreet"),
            $"shredded.SupplyAddress.Locality".as("SupplyAddressLocality"),
            $"shredded.SupplyAddress.DoubleLocality".as("SupplyAddressDoubleLocality"),
            $"shredded.SupplyAddress.Town".as("SupplyAddressTown"),
            $"shredded.SupplyAddress.County".as("SupplyAddressCounty"),
            $"shredded.SupplyAddress.Postcode".as("SupplyAddressPostcode"),
            $"shredded.SupplyAddress.Organisation".as("SupplyAddressOrganisation"),
            $"shredded.SupplyAddress.RegionId".as("SupplyAddressRegionId"),
            
            $"shredded.UpdatedCustomerData.MobileNumber".as("MobileNumber"),
            $"shredded.UpdatedCustomerData.TitleId".as("TitleId"),
            $"shredded.UpdatedCustomerData.FirstName".as("FirstName"),
            $"shredded.UpdatedCustomerData.Surname".as("Surname"),
            $"shredded.UpdatedCustomerData.EmailAddress".as("EmailAddress"),
            $"shredded.UpdatedCustomerData.TelephoneNumber".as("TelephoneNumber"),
            $"shredded.UpdatedCustomerData.DateOfBirth".as("DateOfBirth"),
            $"shredded.UpdatedCustomerData.HasDifferentBillingAddress".as("HasDifferentBillingAddress"),
            
            $"shredded.UpdatedCustomerData.BillingAddress.FlatNo".as("BillingAddressFlatNo"),
            $"shredded.UpdatedCustomerData.BillingAddress.HouseName".as("BillingAddressHouseName"),
            $"shredded.UpdatedCustomerData.BillingAddress.HouseNumber".as("BillingAddressHouseNumber"),
            $"shredded.UpdatedCustomerData.BillingAddress.Thoroughfare".as("BillingAddressThoroughfare"),
            $"shredded.UpdatedCustomerData.BillingAddress.Street".as("BillingAddressStreet"),
            $"shredded.UpdatedCustomerData.BillingAddress.Locality".as("BillingAddressLocality"),
            $"shredded.UpdatedCustomerData.BillingAddress.DoubleLocality".as("BillingAddressDoubleLocality"),
            $"shredded.UpdatedCustomerData.BillingAddress.Town".as("BillingAddressTown"),
            $"shredded.UpdatedCustomerData.BillingAddress.County".as("BillingAddressCounty"),
            $"shredded.UpdatedCustomerData.BillingAddress.Postcode".as("BillingAddressPostcode"),
            $"shredded.UpdatedCustomerData.BillingAddress.Organisation".as("BillingAddressOrganisation"),
            $"shredded.UpdatedCustomerData.BillingAddress.RegionId".as("BillingAddressRegionId"),
            
            $"shredded.UpdatedCustomerData.MpanData.MpanLower".as("MpanLower"),
            $"shredded.UpdatedCustomerData.MpanData.MpanTopLine".as("MpanTopLine"),
            $"shredded.UpdatedCustomerData.MpanData.MeterSerialElec".as("MpanMeterSerialElec"),
            $"shredded.UpdatedCustomerData.MpanData.Source".as("MpanSource"),
            
            $"shredded.UpdatedCustomerData.MprnData.Mprn".as("Mprn"),
            $"shredded.UpdatedCustomerData.MprnData.MeterSerialGas".as("MprnMeterSerialGas"),
            $"shredded.UpdatedCustomerData.MprnData.Source".as("MprnSource"),
            
            $"IsTest",
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
