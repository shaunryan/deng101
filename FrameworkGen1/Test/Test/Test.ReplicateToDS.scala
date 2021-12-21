// Databricks notebook source
import com.gocogroup.metadata._

// COMMAND ----------

// MAGIC %md 
// MAGIC ##Configure##

// COMMAND ----------

dbutils.widgets.text("rundate", "", "rundate (yyyy-MM-dd)")
dbutils.widgets.text("pipeline_name", "", "Pipeline Name")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run Id")
dbutils.widgets.text("source_database", "", "Source DB")
dbutils.widgets.text("source_table", "", "Source Table")
dbutils.widgets.text("destination_location", "", "To Location")


// COMMAND ----------


/*
  pull out the date - this will be a string so could be anything
  therefore parse it to a date to ensure that it's a valid date
*/
val prundate = dbutils.widgets.get("rundate")
val ppipeline_name = dbutils.widgets.get("pipeline_name")
val ppipeline_run_id = dbutils.widgets.get("pipeline_run_id")
val psource_database = dbutils.widgets.get("source_database")
val psource_table = dbutils.widgets.get("source_table")
val pdestination_location = dbutils.widgets.get("destination_location")



// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##Redaction Configuration
// MAGIC 
// MAGIC Declare the tables and fields to be redacted during the replication

// COMMAND ----------

val energyprofile = Seq(
  "SupplyFlatNo", 
  "SupplyHouseNumber", 
  "SupplyHouseName", 
  "SupplyThoroughfare", 
  "SupplyStreet", 
  "SupplyLocality", 
  "SupplyDoubleLocality", 
  "SupplyMpan", 
  "SupplyMprn", 
  "Mpan", 
  "Mprn"
)

val switch = Seq(
  "MeterSerialElec",
  "MpanLower",
  "MpanTopLine",
  "MeterSerialGas",
  "Mprn",
  "BillingAddressFlatNo",
  "BillingAddressHouseName",
  "BillingAddressHouseNumber",
  "BillingAddressThoroughfare",
  "BillingAddressStreet",
  "BillingAddressLocality",
  "BillingAddressDoubleLocality",
  "PreviousAddresses"
)

// place it in a map lookup so we can access by table name.
val redact = Map(
  "energyprofile" -> energyprofile,
  "switch" -> switch
)




// COMMAND ----------

// MAGIC %md 
// MAGIC ##Replicate##

// COMMAND ----------

val r = Replicate(
  ppipeline_run_id,
  ppipeline_name,
  prundate,
  psource_database,
  psource_table,
  pdestination_location
)

// COMMAND ----------

r.replicate(redact, prundate)

// COMMAND ----------

dbutils.notebook.exit("success")
