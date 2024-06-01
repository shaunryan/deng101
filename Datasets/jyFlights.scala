// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC # Stage Flights Data
// MAGIC
// MAGIC To perform these DataFrame operations, we’ll first prepare some data. In the following code snippet, we:
// MAGIC
// MAGIC Import two files and create two DataFrames, one for airport (airportsna) information and one for US flight delays (departureDelays).
// MAGIC
// MAGIC Using expr(), convert the delay and distance columns from STRING to INT.
// MAGIC
// MAGIC Create a smaller table, foo, that we can focus on for our demo examples; it contains only information on three flights originating from Seattle (SEA) to the destination of San Francisco (SFO) for a small time range.

// COMMAND ----------


// In Scala
import org.apache.spark.sql.functions._

// Set file paths
val delaysPath = 
  "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
val airportsPath = 
  "/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"

// Obtain airports data set
val airports = spark.read
  .option("header", "true")
  .option("inferschema", "true")
  .option("delimiter", "\t")
  .csv(airportsPath)
airports.createOrReplaceTempView("airports_na")

// Obtain departure Delays data set
val delays = spark.read
  .option("header","true")
  .csv(delaysPath)
  .withColumn("delay", expr("CAST(delay as INT) as delay"))
  .withColumn("distance", expr("CAST(distance as INT) as distance"))
delays.createOrReplaceTempView("departureDelays")

// Create temporary small table
val foo = delays.filter(
  expr("""origin == 'SEA' AND destination == 'SFO' AND 
      date like '01010%' AND delay > 0"""))
foo.createOrReplaceTempView("foo")
