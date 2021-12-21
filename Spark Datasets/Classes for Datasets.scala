// Databricks notebook source
// MAGIC %md
// MAGIC ## Scala Case Classes
// MAGIC 
// MAGIC The names of the class fields have to be exactly the same

// COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/samples/"))

// COMMAND ----------

case class Bloggers(id:Int, first:String, last:String, url:String, date:String, hits:Int, campaigns:Array[String])

val bloggers = "../data/bloggers.json"

val bloggersDs = spark
  .read
  .format("json")
  .option("path", bloggers)
  .load()
  .as[Bloggers]



// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Working with DataSets
// MAGIC 
// MAGIC Create a dataset dynmically to work with

// COMMAND ----------

import scala.util.Random._

case class Usage(uid:Int, uname:String, usafe:Int)

val r = new scala.util.Random(42)

val data = for (i <- 0 to 1000)
   yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000)))

// create a dataset of Usage typed data
val dsUsage = spark.createDataset(data)
display(dsUsage.limit(10))
