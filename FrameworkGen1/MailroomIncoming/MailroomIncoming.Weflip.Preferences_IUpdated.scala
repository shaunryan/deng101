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
import scala.util.Try

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

def addPreference(df: DataFrame, preference: String, dataType: String) : DataFrame =
{
  if (Try(df(preference)).isSuccess)
  {
    val colTyped = s"${preference}Typed"
    df.withColumn(colTyped, col(preference).cast(dataType))
      .drop(preference)
      .withColumnRenamed(colTyped, preference)
  }
  else
  {
    df.withColumn(preference, lit(null).cast(dataType))
  }
}




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
          .withColumn("IsTest", expr("if(instr(lcase(shredded.SubscriberKey), '-test') > 0, true, false)"))
          .withColumn("Preference", expr("explode(shredded.Preferences)"))
          .where(col("IsValid"))
          .withColumn("PreferenceShortName", expr("""
            CASE Preference.UniqueId
              WHEN 1 THEN 'PriceOnly'
              WHEN 2 THEN 'KnownBrandsOnly'
              WHEN 3 THEN 'GreenTariffsOnly'
              WHEN 4 THEN 'RatedByCustomersOnly'
              WHEN 5 THEN 'AllTariffs'
              WHEN 6 THEN 'FixedTariffsOnly'
              WHEN 7 THEN 'VariableTariffsOnly'
              WHEN 8 THEN 'SavingsThreshold'
              WHEN 9 THEN 'SavingsThreshold'
              WHEN 10 THEN 'SavingsThreshold'
              WHEN 11 THEN 'SavingsThreshold'
              WHEN 12 THEN 'SavingsThreshold'
              WHEN 13 THEN 'SupplierBlackList'
            END
          """))
          .withColumn("PreferenceValueJson", expr("explode(Preference.Value)"))
          .withColumn("PreferenceValueShred", expr("cast(from_json(PreferenceValueJson, 'value STRING') as STRING)"))
          .withColumn("PreferenceValue", expr("""
            CASE 
              WHEN Preference.UniqueId BETWEEN 1 AND 7 THEN replace(replace(PreferenceValueShred,'[',''),']','') 
              WHEN Preference.UniqueId BETWEEN 8 AND 12 AND upper(Preference.PreferenceType) = "BOOLEAN" AND PreferenceValueShred = '[false]' THEN null
              WHEN Preference.UniqueId = 8  AND upper(Preference.PreferenceType) = "BOOLEAN" AND PreferenceValueShred = '[true]' THEN '1' 
              WHEN Preference.UniqueId = 9  AND upper(Preference.PreferenceType) = "BOOLEAN" AND PreferenceValueShred = '[true]' THEN '50' 
              WHEN Preference.UniqueId = 10 AND upper(Preference.PreferenceType) = "BOOLEAN" AND PreferenceValueShred = '[true]' THEN '100' 
              WHEN Preference.UniqueId = 11 AND upper(Preference.PreferenceType) = "BOOLEAN" AND PreferenceValueShred = '[true]' THEN '150' 
              WHEN Preference.UniqueId = 12 AND upper(Preference.PreferenceType) = "BOOLEAN" AND PreferenceValueShred = '[true]' THEN '250' 
              WHEN Preference.UniqueId = 13 THEN PreferenceValueShred
              ELSE 'ERROR'
            END
          """))
          .withColumn("PreferenceDataType", expr("""
            CASE 
              WHEN Preference.UniqueId = 1  THEN upper(Preference.PreferenceType) 
              WHEN Preference.UniqueId = 2  THEN upper(Preference.PreferenceType) 
              WHEN Preference.UniqueId = 3  THEN upper(Preference.PreferenceType) 
              WHEN Preference.UniqueId = 4  THEN upper(Preference.PreferenceType) 
              WHEN Preference.UniqueId = 5  THEN upper(Preference.PreferenceType) 
              WHEN Preference.UniqueId = 6  THEN upper(Preference.PreferenceType) 
              WHEN Preference.UniqueId = 7  THEN upper(Preference.PreferenceType) 
              WHEN Preference.UniqueId BETWEEN 8 AND 12 THEN 'INT'
              WHEN Preference.UniqueId = 13 THEN upper(Preference.PreferenceType)
              ELSE 'ERROR'
            END
          """))
         .where("PreferenceValue IS NOT NULL")
         .select(
            $"UUID",
            $"shredded.CustomerId".as("CustomerId"), 
            $"shredded.AffiliateID".as("AffiliateID"), 
            $"shredded.EncryptedCustomerId".as("EncryptedCustomerId"), 
            $"shredded.SubscriberKey".as("SubscriberKey"), 
            $"shredded.Timestamp".as("Timestamp"),
            $"PreferenceShortName".as("PreferenceName"),
            $"Preference.UniqueId".as("PreferenceUniqueId"),
            //$"PreferenceDataType".as("PreferenceType"),
            $"PreferenceValue",
            $"BatchRundate"
           )
        .groupBy($"UUID", $"CustomerId", $"AffiliateID", $"EncryptedCustomerId", $"SubscriberKey", $"Timestamp", $"BatchRundate")//, $"PreferenceType")
        .pivot("PreferenceName")
        .agg(max($"PreferenceValue"))

//val dfPrep? = addPreference(dfPrep?, "PriceOnly", "BOOLEAN")
val dfPrep1 = addPreference(dfPrep,  "SavingsThreshold", "DECIMAL")
val dfPrep2 = addPreference(dfPrep1, "GreenTariffsOnly", "BOOLEAN")
val dfPrep3 = addPreference(dfPrep2, "KnownBrandsOnly" , "BOOLEAN")
//val dfPrep? = addPreference(dfPrep?, "IsRatedCustomer", "BOOLEAN")
//val dfPrep? = addPreference(dfPrep?, "AllTariffs", "BOOLEAN")
//val dfPrep? = addPreference(dfPrep?, "FixedTariffs", "BOOLEAN")
//val dfPrep? = addPreference(dfPrep?, "VariableTariffs", "BOOLEAN")

val dfPrepFinal = dfPrep3
        .withColumn("LoadDate", lit(loaddate))
        .withColumn("Key", (lit(keyseed) + row_number().over(windowSpecKey).cast("string")).cast("bigint"))
        .withColumn("PipelineRunId", lit(metadata.lpipelineRunId)) 
        .select(
            $"UUID",
            $"CustomerId", 
            $"AffiliateID", 
            $"EncryptedCustomerId", 
            $"SubscriberKey", 
            $"Timestamp",
            $"SavingsThreshold",
            $"GreenTariffsOnly",
            $"KnownBrandsOnly",
            $"LoadDate",
            $"PipelineRunId",
            $"BatchRundate"
        )

dfPrepFinal.write.option("badRecordsPath", metadata.writeExceptionPath).insertInto(metadata.destinationObject)
val filterDst = DataPrePrep.GetFilterPartition(prundate, "BatchRundate")
val writeRowCount = spark.sql(s"""select count(*) from ${metadata.destinationObject} ${filterDst}""").first.getLong(0)

metadata.HandleWriteExceptions(writeRowCount)


// COMMAND ----------

DataEngineeringControl.showNotebookExecutionReport(metadata.lpipelineRunId, notebook)

// COMMAND ----------

dbutils.notebook.exit(s"${notebook} #${dbutils.notebook.getContext.currentRunId} : success")
