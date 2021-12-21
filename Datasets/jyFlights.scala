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

// MAGIC %scala
// MAGIC 
// MAGIC // In Scala
// MAGIC import org.apache.spark.sql.functions._
// MAGIC 
// MAGIC // Set file paths
// MAGIC val delaysPath = 
// MAGIC   "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
// MAGIC val airportsPath = 
// MAGIC   "/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"
// MAGIC 
// MAGIC // Obtain airports data set
// MAGIC val airports = spark.read
// MAGIC   .option("header", "true")
// MAGIC   .option("inferschema", "true")
// MAGIC   .option("delimiter", "\t")
// MAGIC   .csv(airportsPath)
// MAGIC airports.createOrReplaceTempView("airports_na")
// MAGIC 
// MAGIC // Obtain departure Delays data set
// MAGIC val delays = spark.read
// MAGIC   .option("header","true")
// MAGIC   .csv(delaysPath)
// MAGIC   .withColumn("delay", expr("CAST(delay as INT) as delay"))
// MAGIC   .withColumn("distance", expr("CAST(distance as INT) as distance"))
// MAGIC delays.createOrReplaceTempView("departureDelays")
// MAGIC 
// MAGIC // Create temporary small table
// MAGIC val foo = delays.filter(
// MAGIC   expr("""origin == 'SEA' AND destination == 'SFO' AND 
// MAGIC       date like '01010%' AND delay > 0"""))
// MAGIC foo.createOrReplaceTempView("foo")
