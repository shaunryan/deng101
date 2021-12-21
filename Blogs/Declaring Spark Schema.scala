// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # Spark Schema - Best Practice
// MAGIC 
// MAGIC Data has a schema. Data processing technologies may or may not allow the following:
// MAGIC  1. No schema at all, or inferring the schema
// MAGIC  2. Schema on write
// MAGIC  3. Schema on read
// MAGIC 
// MAGIC  
// MAGIC Spark can do all 3 of these, however here we'll focus on inferring the schema and schema on read
// MAGIC 
// MAGIC Not declaring and inferring the schema may appeal to a data novice due it's apparent convenience and it can be quite handy for initial foray's of datasets and troubleshooting. After all you need to get the data in somehow to understand how to apply a schema in the 1st place since it's not always given to you and when it is the data providers are usually over confident about it's quality.
// MAGIC 
// MAGIC Schemas change and maintaining seems like a huge annoying overhead. Fundamentals of computer and information science (physics) demands that data has schemas. Solving the problem by ignoring them is kicking the can down the road. Caring about them is essential and it should not be overwhelmingly onerous to declare or maintain them. The trick is consistently using good tools, development workflows and if you can a good framework.
// MAGIC 
// MAGIC Providing no schema at all is bad! You really don't want this in a production system. Why?
// MAGIC 
// MAGIC 1. Your data has a schema regardless! Using no schema at all says that you don't care about your data and the resulting outcomes of it's use!
// MAGIC 2. In order to infer the schema spark will load the data twice, fine for an adhoc foray but not what you want consistently on bigger volumes in production.
// MAGIC 
// MAGIC Using inferred schema reads with JSON is very common for some reason. I'm note sure why I guess it's because they can be quite complex types and it seems like more work. I going to address that in this article to show you what I find is the best way to manage schema and create them with very little effort, even complex JSON schema.
// MAGIC 
// MAGIC Just a quick note on schema on write. Obviously for a curated datasets, schema on write is generally what you want using a solid format like parquet that takes care of format concerns and schema definition. Unfortunately we don't always get data this way and it's our job to curate it into these high performance formats with better usability among other things.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## The Inferred Schema Load
// MAGIC 
// MAGIC This is naughty! Should never pass a production Pull Request (PR). Notice that data schema and format are 2 separate metadata concerns. Even though we have explicitly declared the formats we have not declared the schema.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC // text
// MAGIC val inferSchema = "true"
// MAGIC val columnNamesIfr = "true"
// MAGIC val columnDelimiter = ","
// MAGIC val escapeCharacter = "\""
// MAGIC val quoteCharacter = "\""
// MAGIC val csvPath = "dbfs:/databricks-datasets/samples/population-vs-price/data_geo.csv"
// MAGIC 
// MAGIC val df = spark.read
// MAGIC   .format("csv")
// MAGIC   .option("inferSchema", inferSchema)
// MAGIC   .option("header", columnNamesIfr)
// MAGIC   .option("delimiter", columnDelimiter)
// MAGIC   .option("escape", escapeCharacter)
// MAGIC   .option("quote", quoteCharacter)
// MAGIC   .load(csvPath)
// MAGIC 
// MAGIC display(df)

// COMMAND ----------

// json - inferred data reads are extremely common even in production!
val inferSchema = "true"
val jsonPath = "dbfs:/databricks-datasets/definitive-guide/data/activity-data/part-00000-tid-730451297822678341-1dda7027-2071-4d73-a0e2-7fb6a91e1d1f-0-c000.json"

val df = spark.read
  .option("inferSchema", inferSchema)
  .json(jsonPath)

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Schema On Read
// MAGIC 
// MAGIC The best practice way is to define a schema for performance and quality reasons. This is known as ***Schema On Read***
// MAGIC 
// MAGIC Also notice the `badRecordsPath` is declared and there are no options to force bad data to load, we want bad data that doesn't meet the schema to flow into an exception dataset on the load; otherwise you really are saying that you truely don't care about your data. I won't cover exception loads here.
// MAGIC 
// MAGIC The schema is commented out here because we haven't declared one yet. We'll do that in what follows.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC // text
// MAGIC val inferSchema = "true"
// MAGIC val columnNamesIfr = "true"
// MAGIC val columnDelimiter = "|"
// MAGIC val escapeCharacter = "\""
// MAGIC val quoteCharacter = "\""
// MAGIC val exceptionPath = "dbfs:/databricks-datasets/exceptions"
// MAGIC val csvPath = "dbfs:/databricks-datasets/samples/population-vs-price/data_geo.csv"
// MAGIC 
// MAGIC spark.read
// MAGIC   .format("csv")
// MAGIC   //.schema(sourceSchema) // defining the schema
// MAGIC   .option("badRecordsPath", exceptionPath)
// MAGIC   .option("header", columnNamesIfr)
// MAGIC   .option("delimiter", columnDelimiter)
// MAGIC   .option("escape", escapeCharacter)
// MAGIC   .option("quote", quoteCharacter)
// MAGIC   .load(csvPath)
// MAGIC 
// MAGIC display(df)

// COMMAND ----------

// json - with a schema!
val jsonPath = "dbfs:/databricks-datasets/samples/people/people.json"
val exceptionPath = "dbfs:/databricks-datasets/definitive-guide/data/activity-data/part-00000-tid-730451297822678341-1dda7027-2071-4d73-a0e2-7fb6a91e1d1f-0-c000.json"

spark.read
  //.schema(sourceSchema)
  .option("badRecordsPath", exceptionPath)
  .json(jsonPath)

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Create Schemas
// MAGIC 
// MAGIC There's a few different ways to programmatically create schemas
// MAGIC 1. Using StructType & StructField to compose the schema
// MAGIC 2. Using StructType methods to compose the schema
// MAGIC 3. Using a DDL String
// MAGIC 4. Using a JSON spark schema definition
// MAGIC 5. Using Case Classes which breaches the realm of datasets vs dataframes. (not covered)
// MAGIC 
// MAGIC I won't cover option 5 here because ***don't use datasets*** unless you really need to. If you don't understand why you need to then ***don't use datasets***. Basically, you get an extra level of type safety, however the price you pay in performance is pretty bad. You can read about the differences [here](https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html)
// MAGIC 
// MAGIC ***In my opinion don't use 1 and 2***, why? You'll see in the examples that follow that this involves coding very specific schema's before declaring and loading the data frame. This is bad because:
// MAGIC   - Clean code! The resulting readability and maintenance is horrible. Perhaps not demonstrated too much in the examples here but most of your data won't be this narrow and simple. You'll end up squinting at a massive list columns amongst your ETL code using tools not designed for that. Not nice at all, especially with complex JSON. You'll soon lose the will to live.
// MAGIC   - Code re-use and clean seperation of transformation code and schema's. Do you really want to package and release your code every time the schema changes. A good principle for software engineering is to separate those things that change a lot from those things that don't. This goes a long way towards allowing good engineering practices around testing, re-use, CI/CD, release cadence i.e. the risk management and robustness of your systems!
// MAGIC   
// MAGIC ***3 and 4 is better because we can pass them as parameters into the job.*** This allows us to separate and maintain schemas in different source code repository than the ETL code. This is great because (not exhaustive):
// MAGIC  - Schema's can be maintained and released on their own cadence separate to the other assets
// MAGIC  - The ETL jobs's themselves have less changes and make good use of code re-use
// MAGIC  - The 2 preceding benefits allows for better testing and reduced amount of testing for things that haven't changed
// MAGIC  - Better and more usable tools can be used to maintain the schema's which makes the development workflow more enjoyable, productive and less error prone
// MAGIC  - (My favourite one) I don't have to type them out, I can code generate them!
// MAGIC 
// MAGIC ***Ultimately using (4) spark JSON schema is the best.*** The DDL string can be parameterized but for large complex schemas the spark JSON definition is better due to the availability of better tooling, ease of maintenance and you can also define descriptive metadata. What I typically do with JSON spark schemas is:
// MAGIC - Organise them into their own repo
// MAGIC - Maintain them using a professional JSON IDE
// MAGIC - CI/CD them onto the lake storage
// MAGIC - They can then be loaded from the lake storage into the ETL jobs and converted to schemas with a re-usable code component
// MAGIC 
// MAGIC ***Warning:*** Just be careful overwriting files on eventually persistent storage with the same names! It needs to have enough time to replicate before you read them... otherwise you may get some failures where the old schema was read from old replica. Doesn't need long but you may run into it if you have automated CI/CD jobs, typically though cloud deployments aren't that fast and test jobs take a little while to provision!
// MAGIC 
// MAGIC Enough of the chit chat here's the examples. Don't overlook the nice bit at the end of this knowledge share where we discuss ***programmatically creating the schemas!*** Don't type them out, otherwise you'll quickly learn to hate data engineering! Use the tools wisely and you'll spend more time having fun and have less mistakes!
// MAGIC 
// MAGIC ***Tip:*** On the loading twice thing! Where we declare schemas below, look at the amount of steps it takes to load the data and compare that with the job steps where we inferred the schema!

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### 1. Using StructType & StructField to compose the schema
// MAGIC 
// MAGIC The workflow for declaring the schema is to load the data with the schema inferred, look at the schema, evaluate whether that's sensible or whether we can tighten it up (it generally does a pretty good job) and then type it out. 
// MAGIC 
// MAGIC 1. This isn't a great workflow for large complex tables, I have to type out the full schema!
// MAGIC 2. It's very specific code that could otherwise be generic
// MAGIC 3. Horrible to maintain in scala and scala IDE tools; even worse in a notebook
// MAGIC 
// MAGIC [See the StructType documentation](https://spark.apache.org/docs/3.0.1/api/java/org/apache/spark/sql/types/StructType.html)
// MAGIC 
// MAGIC It resides in this package ***org.apache.spark.sql.types***

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC import org.apache.spark.sql.types._
// MAGIC 
// MAGIC // json - with a schema!
// MAGIC val jsonPath = "dbfs:/databricks-datasets/definitive-guide/data/activity-data/part-00000-tid-730451297822678341-1dda7027-2071-4d73-a0e2-7fb6a91e1d1f-0-c000.json"
// MAGIC val exceptionPath = "dbfs:/databricks-datasets/exceptions"
// MAGIC 
// MAGIC // but this is very specific code for something that could otherwise be generic, and horrible to maintain in scala IDE or noteboook
// MAGIC // it takes the form :
// MAGIC // StructField(
// MAGIC //     name: String,
// MAGIC //     dataType: DataType,
// MAGIC //     nullable: Boolean = true,
// MAGIC //     metadata: Metadata = Metadata.empty
// MAGIC // )
// MAGIC val sourceSchema = StructType(Array(
// MAGIC     StructField("Arrival_Time",LongType,true),
// MAGIC     StructField("Creation_Time",LongType,true),
// MAGIC     StructField("Device",StringType,true),
// MAGIC     StructField("Index", LongType, true),
// MAGIC     StructField("Model", StringType, true),
// MAGIC     StructField("User", StringType, true),
// MAGIC     StructField("gt", StringType, true),
// MAGIC     StructField("x", DoubleType, true),
// MAGIC     StructField("y", DoubleType, true),
// MAGIC     StructField("z", DoubleType, true)
// MAGIC   ))
// MAGIC 
// MAGIC val df =spark.read
// MAGIC   .schema(sourceSchema)
// MAGIC   .option("badRecordsPath", exceptionPath)
// MAGIC   .json(jsonPath)
// MAGIC 
// MAGIC display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### 2. Using StructType methods to compose the schema
// MAGIC 
// MAGIC The workflow for declaring the schema is to load the data with the schema inferred, look at the schema, evaluate whether that's sensible or whether we can tighten it up (it generally does a pretty good job) and then type it out. 
// MAGIC 
// MAGIC 1. This isn't a great workflow for large complex tables, I have to type out the full schema!
// MAGIC 2. It's very specific code that could otherwise be generic
// MAGIC 3. Horrible to maintain in scala and scala IDE tools; even worse in a notebook
// MAGIC 
// MAGIC [See the StructType documentation](https://spark.apache.org/docs/3.0.1/api/java/org/apache/spark/sql/types/StructType.html)
// MAGIC 
// MAGIC It resides in this package ***org.apache.spark.sql.types***
// MAGIC 
// MAGIC [The add method is used to compose the schema](https://spark.apache.org/docs/3.0.1/api/java/org/apache/spark/sql/types/StructType.html#add-java.lang.String-java.lang.String-boolean-org.apache.spark.sql.types.Metadata-)

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC import org.apache.spark.sql.types._
// MAGIC 
// MAGIC // json - with a schema!
// MAGIC val jsonPath = "dbfs:/databricks-datasets/definitive-guide/data/activity-data/part-00000-tid-730451297822678341-1dda7027-2071-4d73-a0e2-7fb6a91e1d1f-0-c000.json"
// MAGIC val exceptionPath = "dbfs:/databricks-datasets/exceptions"
// MAGIC 
// MAGIC // but this is very specific code for something that could otherwise be generic, and horrible to maintain in scala IDE or noteboook
// MAGIC // it takes the form :
// MAGIC // add(
// MAGIC //     name: String,
// MAGIC //     dataType: DataType,
// MAGIC //     nullable: Boolean = true,
// MAGIC //     metadata: Metadata = Metadata.empty
// MAGIC // )
// MAGIC val sourceSchema = new StructType()
// MAGIC     .add("Arrival_Time",LongType)
// MAGIC     .add("Creation_Time",LongType)
// MAGIC     .add("Device",StringType)
// MAGIC     .add("Index",LongType)
// MAGIC     .add("Model",StringType)
// MAGIC     .add("User",StringType)
// MAGIC     .add("gt",StringType)
// MAGIC     .add("x",DoubleType)
// MAGIC     .add("y",DoubleType)
// MAGIC     .add("z",DoubleType)
// MAGIC 
// MAGIC val df =spark.read
// MAGIC   .schema(sourceSchema)
// MAGIC   .option("badRecordsPath", exceptionPath)
// MAGIC   .json(jsonPath)
// MAGIC 
// MAGIC display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### 3. Using a DDL String
// MAGIC 
// MAGIC The workflow for declaring the schema is to load the data with the schema inferred, look at the schema, evaluate whether that's sensible or whether we can tighten it up (it generally does a pretty good job) and then type it out. 
// MAGIC 
// MAGIC 1. This isn't a great workflow for large complex tables, I have to type out the full schema!
// MAGIC 2. The DDL string is specific but we can pull that out of the code and provide it as a parameter form separate configuration, great our code is now generic and schema's are maintained separately!
// MAGIC 3. There are no tools for DDL strings so still a bit horrible to maintain externally in configuration.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC import org.apache.spark.sql.types.StructType
// MAGIC 
// MAGIC // json - with a schema!
// MAGIC val jsonPath = "dbfs:/databricks-datasets/definitive-guide/data/activity-data/part-00000-tid-730451297822678341-1dda7027-2071-4d73-a0e2-7fb6a91e1d1f-0-c000.json"
// MAGIC val exceptionPath = "dbfs:/databricks-datasets/exceptions"
// MAGIC 
// MAGIC // Much more generic! We can provide this string as a parameter from schema's maintained else where.
// MAGIC val ddl = """
// MAGIC   `Arrival_Time` BIGINT, 
// MAGIC   `Creation_Time` BIGINT,
// MAGIC   `Device` STRING,
// MAGIC   `Index` BIGINT,
// MAGIC   `Model` STRING,
// MAGIC   `User` STRING,
// MAGIC   `gt` STRING,
// MAGIC   `x` DOUBLE,
// MAGIC   `y` DOUBLE,
// MAGIC   `z` DOUBLE
// MAGIC """
// MAGIC val sourceSchema = StructType.fromDDL(ddl)
// MAGIC 
// MAGIC val df =spark.read
// MAGIC   .schema(sourceSchema)
// MAGIC   .option("badRecordsPath", exceptionPath)
// MAGIC   .json(jsonPath)
// MAGIC 
// MAGIC display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### 4. Using a JSON spark schema definition - My Preferred Option
// MAGIC 
// MAGIC The workflow for declaring the schema is to load the data with the schema inferred, ***code generate the schema by exporting it***, review whether that's sensible or whether we can tighten it up (it generally does a pretty good job) and then copy it to a project maintained using a good quality JSON IDE. 
// MAGIC 
// MAGIC 1. This is a much better workflow for complex tables! I used this on very large JSON docs with no problem at all.
// MAGIC 2. The JSON schema is specific but we can pull that out of the code and provide it as a parameter from configuration, great our code is now generic and schemas are maintained seperately!
// MAGIC 3. There are some fantastic free and paid IDE tools for maintaining and validating JSON; even if you have no budget, checkout the VSCode plugins.
// MAGIC 
// MAGIC ***Tip:*** JSON data schema can have 2 representations of null. The element could be present with a value of null or the element could be missing entirely. It's entirely OK in the JSON parser for a JSON attribute to be missing and to some extent this is why JSON can be great for modelling objects with sparse attributes. The great news is spark, parquet and deltalake supports the same complex types and supporting missing attributes. ***However*** because of this when you generate your spark schemas make sure you provide a sample with a full definition of all the attributes and that the attributes have values otherwise you'll miss attributes in the schema entirely or miss their datatype. 
// MAGIC 
// MAGIC That being said, it's not hard or onerous to profile the data to discover this and you should be extensively profiling your data anyway for lots of other very good reasons; as a data engineer one of your core concern is to care about the data! That is to be inquisitive about and to understand the data that you've been asked to curate!

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 1 - Create the Schema
// MAGIC 
// MAGIC Infer the schema and then export the schema as JSON. In practice you'd dump this out into GIT project repo, manage with a good quality JSON IDE (e.g. VSCode or JsonBuddy).
// MAGIC 
// MAGIC Obviously this step is pre-production. Also involves having a check of the schema to make sure exactly as you want and as tight as possible.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC import org.apache.spark.sql.types.StructType
// MAGIC 
// MAGIC // json - with a schema!
// MAGIC val jsonPath = "dbfs:/databricks-datasets/definitive-guide/data/activity-data/part-00000-tid-730451297822678341-1dda7027-2071-4d73-a0e2-7fb6a91e1d1f-0-c000.json"
// MAGIC 
// MAGIC val df = spark.read.json(jsonPath)
// MAGIC val schemaJson = df.schema.json
// MAGIC val path = "/FileStore/jsonSchemaDemo.json"
// MAGIC 
// MAGIC dbutils.fs.put(path, schemaJson) 

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 2 - Load the json using the schema
// MAGIC 
// MAGIC Once you have all your schemas in a project with good maintenance worflows, testing and CI/CD the following generic code can be used to load data using schemas. Obviously where there are literal variables use parameters.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC import org.apache.spark.sql.types.StructType
// MAGIC import org.apache.spark.sql.types.DataType
// MAGIC import scala.io.Source
// MAGIC 
// MAGIC // json - with a schema!
// MAGIC val jsonPath = "dbfs:/databricks-datasets/definitive-guide/data/activity-data/part-00000-tid-730451297822678341-1dda7027-2071-4d73-a0e2-7fb6a91e1d1f-0-c000.json"
// MAGIC val exceptionPath = "dbfs:/databricks-datasets/exceptions"
// MAGIC 
// MAGIC // Much more generic and maintainable, we just the need the path of the schema as an additional parameter and load it
// MAGIC val schemaPath = "/dbfs/FileStore/jsonSchemaDemo.json"
// MAGIC val sourceSchemaJson = Source.fromFile(schemaPath).getLines.mkString
// MAGIC // convert the json to a schema
// MAGIC val sourceSchema = DataType.fromJson(sourceSchemaJson).asInstanceOf[StructType]
// MAGIC 
// MAGIC // Vola! Use it to load the data
// MAGIC val df =spark.read
// MAGIC   .schema(sourceSchema)
// MAGIC   .option("badRecordsPath", exceptionPath)
// MAGIC   .json(jsonPath)
// MAGIC 
// MAGIC display(df)
// MAGIC 
// MAGIC // clean up - for demo purposes
// MAGIC dbutils.fs.rm("/FileStore/jsonSchemaDemo.json")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Wrap Up
// MAGIC 
// MAGIC So the astute veterans among you coming from a SQL world my be think! How is this any better? This doesn't solve anything? Why id this good? I thought JSON was loosely coupled. We still have to declare and manage data schemas, and what about [JSON schema's](https://json-schema.org/)? where do they fit in?
// MAGIC 
// MAGIC Actually the only thing that's changed is:
// MAGIC - Separation of compute and storage
// MAGIC - Standardisation of data file formats
// MAGIC - Elastic linear scalability
// MAGIC - The ability to do schema on read as well as write
// MAGIC 
// MAGIC However data still has schemas and we need to create and maintain them for quality, usability and performance reasons!
// MAGIC 
// MAGIC However bear with me since this paradigm does has significant advantages, it's just that we have to put a little thought and design into it:
// MAGIC - JSON is loosely coupled and we can benefit from that
// MAGIC - JSON doesn't care what order the columns are in
// MAGIC - Deltalake has [schema evolution features](https://databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html) that we can use
// MAGIC - Cloud storage is dirt cheap
// MAGIC - Spark is completely open JVM platform and we can write custom distributable UDF's
// MAGIC - JSON Schema validation libraries are openly available
// MAGIC 
// MAGIC In my next write I'll show how we can create a completely balanced solution:
// MAGIC - All data frames are loaded efficiently and robustly using spark schema
// MAGIC - All raw data is validated using highly scalable JSON schema validation function
// MAGIC - Format errors are excluded to exception tables
// MAGIC - Schema invalid data is jailed at the initial raw layer - no mess to clean up
// MAGIC - New fields JSON documents are automatically ingested and if we want to take them further up stream we adjust are schema's and tables will automatically adjust to changes
// MAGIC 
// MAGIC In terms of formats, if you get the opportunity to request a specific format the order of preference:
// MAGIC 1. Parquet or a structured or semi-structured format that has metadata
// MAGIC 2. If it has to be text then go for JSON line, JSON line is much better than CSV since it has some types and metadata schema standard
// MAGIC 3. XML line though I can't imagine that would be easier for a supplier than json line
// MAGIC 5. csv formats should be avoided, since there is no metadata standard and formats can be troublesome since although you can escape column delimiters quoted you cannot escape row delimiters! Also explicit column ordering is a huge pain, for this reason I've written middle ware before that converts them to JSON in order to standardise the text ingest of the data platform
// MAGIC 4. JSON or XML multi-line should be avoided, it's a bit of performance pain in the neck
