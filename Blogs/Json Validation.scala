// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC First of all lets create some json data. Note this example for loading line by line json! That means each text line has an individual json document on it. You could use the same approach with some tweaks to a multiline document.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val data = """
// MAGIC {"id": 1, "first_name": "Idaline", "last_name": "Laurenson", "email": "ilaurenson0@ifeng.com","gender": "Female","movies": ["Walk All Over Me","Master and Commander: The Far Side of the World","Show Me Love","Touch of Evil"],"job_title": "Librarian","rating": 4.9,"rated_on": "2019-03-16T01:20:04Z"}
// MAGIC {"id": 2,"first_name": "Deva","last_name": "Paulack","email": "dpaulack1@altervista.org","gender": "Female","movies": ["Batman/Superman Movie, The","Amazing Panda Adventure, The"],"job_title": "Recruiter","rating": 6.8,"rated_on": "2019-11-03T16:14:14Z"}
// MAGIC {"id": 3,"first_name": "Corinna","last_name": "Yesenev","email": "cyesenev2@hubpages.com","gender": "Female","movies": ["Patrice O'Neal: Elephant in the Room","Three Little Words","Capitalism: A Love Story","Flying Tigers"],"job_title": "Tax Accountant","rating": 6.7,"rated_on": "2020-01-30T13:30:04Z"}
// MAGIC {"id": 4,"first_name": "Ludwig","last_name": "Coogan","email": "lcoogan3@cornell.edu","gender": "Male","movies": ["Cry, The (Grido, Il)","Client, The","Vai~E~Vem","Prince of Egypt, The","Merry Widow, The"],"job_title": "Assistant Media Planner","rating": 1.9,"rated_on": "2019-03-13T01:32:55Z"}
// MAGIC {"id": 5,"first_name": "Robbie","last_name": "Ginnane","email": "rginnane4@wp.com","gender": "Male","movies": ["Three Men and a Cradle (3 hommes et un couffin)","American Violet","Goin' South","Crimson Petal and the White, The","In Tranzit"],"job_title": "Nurse Practicioner","rating": 1.7,"rated_on": "2020-02-26T18:24:35Z"}
// MAGIC {"id": 6,"first_name": "Jaquenette","last_name": "Durbridge","email": "jdurbridge5@tuttocitta.it","gender": "Female","movies": ["Calendar","Brave New World"],"job_title": "Quality Control Specialist","rating": 2.6,"rated_on": "2019-08-15T18:25:01Z"}
// MAGIC {"id": 7,"first_name": "Wolf","last_name": "Bernhardt","email": "wbernhardt6@cam.ac.uk","gender": "Male","movies": ["Dr. Giggles","Ulzana's Raid"],"job_title": "Paralegal","rating": 7.6,"rated_on": "2019-09-25T12:03:55Z"}
// MAGIC {"id": 8,"first_name": "Allyn","last_name": "Eykel","email": "aeykel7@google.ru","gender": "Female","movies": ["Wild Guitar","Letter From Death Row, A","John Dies at the End","Joker"],"job_title": "Administrative Officer","rating": 5.0,"rated_on": "2019-07-08T00:42:04Z"}
// MAGIC {"id": 9,"first_name": "Dennie","last_name": "Trevers","gender": "Male","movies": ["Watermark","Mondo Hollywood","Bicycle, Spoon, Apple (Bicicleta, cullera, poma)"],"job_title": "Safety Technician II","rating": 7.3,"rated_on": "2019-09-11T11:49:55Z"}
// MAGIC {"id": 10,"first_name": "Shae","last_name": "Bengal","email": "sbengal9@guardian.co.uk","gender": "Male","movies": null,"job_title": "Structural Analysis Engineer","rating": 8.2,"rated_on": "2019-04-25T17:21:26Z"}
// MAGIC """
// MAGIC 
// MAGIC val path = "/FileStore/ironclad.json"
// MAGIC 
// MAGIC dbutils.fs.rm(path) 
// MAGIC dbutils.fs.put(path, data) 

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Load the data using a simple json load to review it. 

// COMMAND ----------

val path = "/FileStore/ironclad.json"
val df = spark.read.json(path)
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC The problem with using `val df = spark.read.json(path)`:
// MAGIC 
// MAGIC 1. Any post validation will very specfically need to be coded into columns that are created
// MAGIC 2. A common (old) method is to use tight schema coupling which protects the data but leads to micro-manging fields and data load failures
// MAGIC 3. Stemming from 2 the workflow required to easily query the invalid data, fix it in-situ and reload it is often overlooked; if you can't get your unstrctured data into to a place to analyse it's really difficult to fix it. Whilst fixing at source should be the aim; operational needs require pragmatism in the short term.
// MAGIC 4. Handling mixed formats requires a different methods e.g. columns strcutures holding json strings.
// MAGIC 5. There is a very rich [json validation schema standard](https://json-schema.org/), the spark json read doesn't give you the opportunity to use it.
// MAGIC 
// MAGIC Here is an alternative approach. There are others!
// MAGIC 
// MAGIC 1. Load the unstructured json data as a single column of text.
// MAGIC 2. Validate the json using json schema, tag the row with a validation flag and exception message.
// MAGIC 3. Use a spark schema matching the json schema to shred the data into the required format using a short generic expression.
// MAGIC 4. Load the data into a deltalake table with schema evolution turned on.
// MAGIC 
// MAGIC Benefits:
// MAGIC  - Data always lands no matter what, when it's invalid it's flagged and reported. Importantly however it's the in table, we can query it, fix it and just re-run an idemptotant job (because you've built your jobs with idempotance haven't you!).
// MAGIC  - Data is validated using json schema which has far more robust data checking options than a deltalake schema.
// MAGIC  - We can just filter out the bad from flowing further into the information stack, or not, it's a matter of choice and basic configuration.
// MAGIC  - New fields arriving in the raw lake beforehand are alerted, if we want them then we just add them to the json & spark schema and they will automatically flow through into the deltalake table including column creation. We just re-run the idempotant job (because you've built your jobs with idempotance haven't you!).
// MAGIC  - It's minimal, generic and re-usable code for any schema. 
// MAGIC  - The spark & json schema's are configuration held in schema files loaded as needed, I have framework that fits all this together by naming convention and config. This is another key development workflow benefit in that schema's are logically managed, maintained, and git controlled in their own repo separate from the code meaning they can be released at their own cadence. They can also be edited and tested using tools better designed for specifically handling json and json schema's. Debugging large json schema's embeded in notebooks and code is a huge pain and is ugly.
// MAGIC  
// MAGIC Downsides:
// MAGIC  - Json validating is slower. But! validating your data is slower. It is distributed though because in this example it's implemented as UDF. The thing is if you need clean data you need to validate it in easily maintainable way! The data at raw is slow anyway because we Deng's haven't done our magic yet; and the best place to validate and jail raw data is at the front door! Rather unpicking a mess further upstream that may have actually done some damage.
// MAGIC  
// MAGIC 
// MAGIC  

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC # Preparation
// MAGIC 
// MAGIC First of all we need to create:
// MAGIC 
// MAGIC 1. Spark Schema file that is used for auto shredding the raw data
// MAGIC 2. JSON schema file that is used for validating raw data
// MAGIC 3. Add JSON validation library to the cluster that can use the json schema file to validate the json
// MAGIC 
// MAGIC I wouldn't normally do this is a notebook. User better json tools for this. I've simply done it here for the convenience of demonstration and brevity of write up. An observation here is that it's a shame there is no easy way currently to convert a json schema to a spark schema. Something I may write one day.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### 1. Create Spark Schema File
// MAGIC 
// MAGIC I'm using a trick to define it from the inferred load. This is much easier. In real life be sure to review it though to make sure the data types are what you want, add descriptions and make sure you haven't dropped any attributes that weren't serialized because they are optional or null.

// COMMAND ----------

val path = "/FileStore/ironclad.json"
val df = spark.read.json(path)
val sparkSchema = df.schema.json

val schemapath = "/FileStore/ironclad_spark_schema.json"
dbutils.fs.rm(schemapath) 
dbutils.fs.put(schemapath, sparkSchema) 

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### 2. Create Json Schema File
// MAGIC 
// MAGIC It would be nice to generate this from the spark schema we already defined. Something to write one day.

// COMMAND ----------

val jsonSchema = """{
	"definitions": {},
	"$schema": "http://json-schema.org/draft-07/schema#", 
	"$id": "https://example.com/object1600248981.json", 
	"title": "Root", 
	"type": "object",
	"required": [
		"id",
		"first_name",
		"last_name",
		"email",
		"gender",
		"movies",
		"job_title",
		"rating",
		"rated_on"
	],
	"properties": {
		"id": {
			"$id": "#root/id", 
			"title": "Id", 
			"type": "integer"
		},
		"first_name": {
			"$id": "#root/first_name", 
			"title": "First_name", 
			"type": "string"
		},
		"last_name": {
			"$id": "#root/last_name", 
			"title": "Last_name", 
			"type": "string"
		},
		"email": {
			"$id": "#root/email", 
			"title": "Email", 
			"type": "string"
		},
		"gender": {
			"$id": "#root/gender", 
			"title": "Gender", 
			"type": "string"
		},
		"movies": {
			"$id": "#root/movies", 
			"title": "Movies", 
			"type": "array",
			"default": [],
			"items":{
				"$id": "#root/movies/items", 
				"title": "Items", 
				"type": "string"
			}
		},
		"job_title": {
			"$id": "#root/job_title", 
			"title": "Job_title", 
			"type": "string"
		},
		"rating": {
			"$id": "#root/rating", 
			"title": "Rating", 
			"type": "number"
		},
		"rated_on": {
			"$id": "#root/rated_on", 
			"title": "Rated_on", 
			"type": "string",
			"format": "date-time"
		}
	}
}"""

val jsonSchemaPath = "/FileStore/ironclad_json_schema.json"
dbutils.fs.rm(jsonSchemaPath)
dbutils.fs.put(jsonSchemaPath, jsonSchema) 

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### 3. Create the Ingest Schema

// COMMAND ----------

val ingestSchema = """
{"type":"struct","fields":[
  {"name":"data","type":"string","nullable":true,"metadata":{}}
]}"""

val ingestSchemaPath = "/FileStore/ingest_spark_schema.json"
dbutils.fs.rm(ingestSchemaPath)
dbutils.fs.put(ingestSchemaPath, ingestSchema) 

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3. Add the JSON Validation Library

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### Operational Code - Load & Validate the Data!
// MAGIC 
// MAGIC Now that dev workflow is done we can create an operational pipeline does the following:
// MAGIC 
// MAGIC 1. Create the JSON Schema Validation UDF
// MAGIC 2. Load our previously created JSON & Spark Schemas from disk
// MAGIC 3. Load validate and shred the data into a dataframe
// MAGIC 4. Write the data to a delta table
// MAGIC 
// MAGIC **Note:** There's still a few things in this example that are literally declared just to keep it simple and on topic. A real operational pipeline would have bit more configuration injection than what I'm showing here and also logging, alerting and operational stats dashboarding

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### 1. Create the JSON Schema Validation UDF
// MAGIC 
// MAGIC This function can run on a distributed spark data frame. 
// MAGIC It takes a JSON string and a JSON shema string and validates the JSON using the schema.
// MAGIC If the JSON is valid and empty validation string is returned. If the JSON fails the validation
// MAGIC a message is returned with details of the invalid JSON.

// COMMAND ----------

import org.apache.spark.sql.functions._
import util.{Try,Failure,Success}
import org.everit.json.schema.loader.SchemaLoader
import org.everit.json.schema.{Schema => EveritSchema, ValidationException}
import org.json.{JSONObject, JSONTokener}

def udfValidateJson(jsonString:String, schemaString:String) :String =
{
    Try{
      val jsonSchema = new JSONObject(new JSONTokener(schemaString))
      val jsonObject = new JSONObject(new JSONTokener(jsonString))
      val schema:EveritSchema = SchemaLoader.load(jsonSchema)
      schema.validate(jsonObject)
    }
    match {
      case Success(s) => ""
      case Failure(e: ValidationException) => s"Schema Validation Error: ${e.getMessage()}"
      case Failure(e: Throwable) => s"Something went terribly wrong: ${e.getMessage()}"
    }
}

// register the UDF so that we can use it in a spark dataframe
val validateJson = udf( udfValidateJson(_: String, _:String): String)


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### 2. Load the JSON & Spark Schema

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Just a quick review of the schema files we're using:

// COMMAND ----------

display(dbutils.fs.ls("/FileStore/").filter(
  f => 
  
  // This is the json schema for validation
  f.name.matches("ironclad_json_schema.json") 
  
  // This is the spark metadata schema, 
  // we're using this to autoshred the data into a struct
  || f.name.contains("ironclad_spark_schema.json")  
  
  // This is the spark schema of the file format
  // used for a performant read of the text data
  || f.name.contains("ingest_spark_schema.json")    
))

// COMMAND ----------

import scala.io.Source
import java.nio.file.{Files, Paths}
import java.io.IOException

// this is a utility function to help read and close files safely.
object ReadFile{
  def apply(path:String):String = {
    readFile(path)
  }
  // looks complicated but basically just means it takes a resource type A that implements a close method
  // and a function to try and execute using resource type A that returns type B
  private def using[A <: {def close(): Unit}, B](resource: A)(f: A => B): B = {
    try {
      f(resource)
    } finally {
      resource.close()
    }
  }
  
  // read the file closing it up safely
  private def readFile(path: String) = {
    if (!Files.exists((Paths.get(path)))) {
      val msg = s"File doesn't not exist ${path}"
      throw new IOException(msg)
    }
    using(Source.fromFile(path)) {
      source => {
        source.getLines().mkString
      }
    }
  }
}


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### 2. Load, JSON Validate & Shred the JSON into a Spark Struct

// COMMAND ----------

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._

val dataPath = "/FileStore/ironclad.json"

// read in the schema's
val jsonSchemaStr = ReadFile("/dbfs/FileStore/ironclad_json_schema.json")
val dataSparkSchemaStr = ReadFile("/dbfs/FileStore/ironclad_spark_schema.json")
val ingestSparkSchemaStr = ReadFile("/dbfs/FileStore/ingest_spark_schema.json")

// use this to generate a uuid table key
val uuid = udf(() => java.util.UUID.randomUUID().toString)
// Create the schema to load the json file as text efficiently without inferring the read
val sourceSchema = DataType.fromJson(ingestSparkSchemaStr).asInstanceOf[StructType]
// Create the spark schema to autoshred the JSON to a struct from the validated JSON String
val sparkSchema = DataType.fromJson(dataSparkSchemaStr).asInstanceOf[StructType]

// read the json line as text
// doing it this way is also handy if you have a data format that has multiple columns containing json
// if you have mulitple json columns there's a way of validating shredding them generically as a collection
// I'm not giving that away for free though ;)
val df = spark.read
  .format("csv")
  .schema(sourceSchema)
  .option("badRecordsPath", "/dbfs/exceptions/ironclad/")
  .option("header", "false")
  .option("delimiter", "|")
  .option("escape", "\"")
  .option("quote", "\"")
  .load(dataPath)

// set up the column ordering for our derived columns that we want as standard
// add in the columns already in the source data set
val columns: Array[String] = Array("_id", "_filename", "_isValid", "_validationMessage") ++ df.columns ++ Array("ShreddedData")

// now validate and shred the json
// Look how much data governance value we get in such a small amount of code!!!
// This pattern can be used to validate and shred json much bigger and complex documents with no change to this code
// all you need is the spark and json schemas!!
val dfp = df
  .withColumn("_id", uuid())
  .withColumn("_filename", input_file_name())
  // validate the json using our custom UDF
  .withColumn("_validationMessage", validateJson(col("data"),lit(jsonSchemaStr)))
  // derive a validation flag for easy filtering and troubleshooting the load
  .withColumn(s"_isValid", expr(s"cast(if(_validationMessage='',true,false) as BOOLEAN)"))
  // autoshred the json to spark struct!
  .withColumn("shreddedData", from_json($"data", sparkSchema))
  .select(columns.head, columns.tail: _*)



// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### 4. Insert the Data Into A Delta Table

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val tableName = "ironclad"
// MAGIC val tableSchema = "default"
// MAGIC // when wr the data to delta table, create if exist otherwise append
// MAGIC val mode = if (
// MAGIC   spark.sql(s"SHOW TABLES IN $tableSchema")
// MAGIC   .where(s"tableName = '$tableName'")
// MAGIC   .count == 0L) "errorifexists" else "append"
// MAGIC 
// MAGIC 
// MAGIC dfp.write.format("delta")
// MAGIC   .option("mergeSchema", "true")
// MAGIC   .mode(mode)
// MAGIC   .saveAsTable(s"$tableSchema.$tableName")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### Explore the Data
// MAGIC 
// MAGIC Now we've loaded our data lets check it out using SQL.
// MAGIC In an operational setups these loads stats would be alerted to an ops dashboard

// COMMAND ----------

// MAGIC %sql
// MAGIC -- what's the quality of out load.
// MAGIC SELECT 
// MAGIC   IF(_isValid, "Valid", "Invalid") AS Valid,
// MAGIC   count(_id) Number_Of_Records
// MAGIC FROM default.ironclad
// MAGIC GROUP BY if(_isValid, "Valid", "Invalid")

// COMMAND ----------

// MAGIC %md
// MAGIC Inspect the valid data

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT _id, shreddedData
// MAGIC FROM default.ironclad
// MAGIC WHERE _isValid

// COMMAND ----------

// MAGIC %md
// MAGIC Inspect the Invalid data

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT _id, shreddedData, _validationMessage
// MAGIC FROM default.ironclad
// MAGIC WHERE !_isValid
