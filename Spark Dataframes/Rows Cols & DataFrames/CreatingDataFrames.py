# Databricks notebook source
# MAGIC %scala
# MAGIC 
# MAGIC val df = spark.read.format("json")
# MAGIC .load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC df = spark.read.format("json") \
# MAGIC .load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.Row
# MAGIC import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
# MAGIC 
# MAGIC val myManualSchema = new StructType(Array(
# MAGIC   new StructField("some", StringType, true),
# MAGIC   new StructField("col", StringType, true),
# MAGIC   new StructField("names", LongType, false)
# MAGIC ))
# MAGIC 
# MAGIC val myRows = Seq(
# MAGIC   Row("Hello", null, 1L),
# MAGIC   Row("World", null, 2L)
# MAGIC )
# MAGIC 
# MAGIC val myRDD = spark.sparkContext.parallelize(myRows)
# MAGIC val mkDf = spark.createDataFrame(myRDD, myManualSchema)
# MAGIC 
# MAGIC display(mkDf)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC 
# MAGIC val myDf = Seq(
# MAGIC   ("Hello", null, 1L),
# MAGIC   ("World", null, 2L)).toDF("some", "col", "names")
# MAGIC 
# MAGIC display(myDf)

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql import Row
# MAGIC from pyspark.sql.types import StructField, StructType, StringType, LongType
# MAGIC 
# MAGIC myManualSchema = StructType([
# MAGIC   StructField("some", StringType(), True),
# MAGIC   StructField("col", StringType(), True),
# MAGIC   StructField("names", LongType(), False)
# MAGIC ])
# MAGIC 
# MAGIC myRows = [Row("Hello", None, 1), 
# MAGIC           Row("Cruel", None, 2), 
# MAGIC           Row("World", None, 3)]
# MAGIC 
# MAGIC myDf = spark.createDataFrame(myRows, myManualSchema)
# MAGIC display(myDf)

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql import Row
# MAGIC from pyspark.sql.types import StructField, StructType, StringType, LongType
# MAGIC 
# MAGIC data = list(range(10))
# MAGIC 
# MAGIC myDf = spark.createDataFrame(data, LongType())
# MAGIC 
# MAGIC display(myDf)

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql import Row
# MAGIC from pyspark.sql.types import StructField, StructType, StringType, LongType
# MAGIC 
# MAGIC myManualSchema = StructType([
# MAGIC   StructField("some", StringType(), True),
# MAGIC   StructField("complex", StructType([
# MAGIC      StructField("col", StringType(), True),
# MAGIC      StructField("names", LongType(), False)   
# MAGIC   ]),  True)
# MAGIC ])
# MAGIC 
# MAGIC myRows = [Row("Hello", Row(None, 1)),
# MAGIC           Row("Cruel", Row(None, 2)), 
# MAGIC           Row("World", Row(None, 3))]
# MAGIC 
# MAGIC myDf = spark.createDataFrame(myRows, myManualSchema)
# MAGIC display(myDf)

# COMMAND ----------

print(myDf.first()[1][1])
