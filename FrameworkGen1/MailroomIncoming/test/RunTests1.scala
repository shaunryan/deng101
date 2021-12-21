// Databricks notebook source
dbutils.widgets.text("testRunId", "", "Test Run Id")
dbutils.widgets.text("environment", "", "Environment")

// COMMAND ----------

val testRunId = dbutils.widgets.get("testRunId")
val environment = dbutils.widgets.get("environment")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## TestStage.ValidContractLoad

// COMMAND ----------

import org.apache.spark.sql.functions._
val pipeline = "mailroomIngest"

var test = "Stage Row Counts"
var initialLoadTeststage = "Initial Load"
var initialLoadAffiliateId = "DATASCIENCE-UNIT-TEST-InitialLoad"
var initialLoadTest = "\"AffiliateID\":\"DATASCIENCE-UNIT-TEST-InitialLoad\""
var initialLoadRowCount = "500"

// COMMAND ----------


case class table(name: String, explosionFactor: String)
//incorporate explosion factor along with table name
val tables = Seq(new table("iautoswitchoptoutrequested","1"),
                 new table("icancelledswitchresolutionfound","1"),
                 new table("icoolingoffperiodexpired","1"),
                 new table("imanualenergyswitchrequested","1"),
                 new table("isavingsopportunityrejected","1"),
                 new table("isupplierrequestedswitchcancelled","1"),
                 new table("iswitchcompleted","1"),
                 new table("iswitchcompletionunconfirmed","1"),
                 new table("iswitchdetailssenttosupplier","1"),
                 new table("iswitchexpired","1"),
                 new table("iswitchstarted","1"),
                 new table("iuserrequestedswitchcancelfailed","1"),
                 new table("iuserrequestedswitchcancellation","1"),
                 new table("iuserrequestedswitchcancelled","1"),
                 new table("preferences_iupdated","1"))


val stagesql = tables.map(table => s"SELECT '${table.name}' as name, cast(${table.explosionFactor} as INT) as explosionFactor, count(1) as count FROM mailroomincoming.${table.name} WHERE FileDate = '1900-01-01T00:00:00.000+0000' and Data like '%${initialLoadTest}%'")
val stageShredsql = tables.map(table => s"SELECT '${table.name}' as name, cast(${table.explosionFactor} as INT) as explosionFactor, count(1) as count FROM mailroomincoming.weflip_${table.name} WHERE BatchRunDate = '1900-01-01T00:00:00.000+0000' and AffiliateID = '${initialLoadAffiliateId}'")


// COMMAND ----------

    val sql = s"""
    select
        Id,
        RunDate,
        Pipeline,
        TestStage,
        Test,
        Result,
        ResultMeasures,
        if(Result!='Suceeded', 'Stage and ShredStage are expected to be equal to ${initialLoadRowCount}', null) ExceptionDetail,
        PartitionPeriod
    from
    (
      select 
        uuid() as Id,
        now() as RunDate,
        '${pipeline}' as Pipeline,
        '${initialLoadTeststage}' as TestStage,
        concat(stage.name , ' ${test}') as Test,
        CASE        
          WHEN (stage.count != ${initialLoadRowCount}) THEN "Failed" 
          
          --incorporate explosion factor
          WHEN (stage.count * stage.explosionFactor != shredstage.count) THEN "Failed"   
          
          ELSE 'Suceeded'
        END as Result,
        concat('Stage=',cast(stage.count as string),', ShredStage=',cast(shredstage.count as string)) as ResultMeasures,
        cast(date_format(now(), 'yyyyMM') as int) as PartitionPeriod
      FROM
      (
        ${stagesql.mkString(" union all \n")}
        
      ) stage
      JOIN 
      (
        ${stageShredsql.mkString(" union all \n")}
      ) shredstage
      ON stage.name = shredstage.name
    )
    """

    val testresult = spark.sql(sql)
    .withColumn("TestRunId", lit(testRunId))
    .select($"Id",$"TestRunId",$"RunDate",$"Pipeline",$"TestStage",$"Test",$"Result",$"ResultMeasures",$"ExceptionDetail",$"PartitionPeriod")

    testresult.write.insertInto("dataengineeringlog.testresult")

// COMMAND ----------

dbutils.notebook.exit(s"success")
