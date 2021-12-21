// Databricks notebook source
// MAGIC %run AzureDataFactory/Includes/SetupEnvironment

// COMMAND ----------

ConnectToDataLake()

// COMMAND ----------

val rawData = spark.sql("select * from rawweflipenergy.preparedswitch")

// COMMAND ----------


import com.amazon.deequ.profiles.{ColumnProfilerRunner, NumericColumnProfile}

val result = ColumnProfilerRunner()
  .onData(rawData)
  .run()

// COMMAND ----------

    result.profiles.foreach { case (productName, profile) =>

      println(s"Column '$productName':\n " +
        s"\tcompleteness: ${profile.completeness}\n" +
        s"\tapproximate number of distinct values: ${profile.approximateNumDistinctValues}\n" +
        s"\tdatatype: ${profile.dataType}\n")
    }

// COMMAND ----------

    /* For numeric columns, we get descriptive statistics */
    val totalNumberProfile = result.profiles("totalNumber").asInstanceOf[NumericColumnProfile]

    println(s"Statistics of 'totalNumber':\n" +
      s"\tminimum: ${totalNumberProfile.minimum.get}\n" +
      s"\tmaximum: ${totalNumberProfile.maximum.get}\n" +
      s"\tmean: ${totalNumberProfile.mean.get}\n" +
      s"\tstandard deviation: ${totalNumberProfile.stdDev.get}\n")
