# Databricks notebook source
# MAGIC %scala
# MAGIC
# MAGIC val df = spark.read.format("json")
# MAGIC .load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json")

# COMMAND ----------


df = spark.read.format("json") \
.load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json")

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

from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType

myManualSchema = StructType([
  StructField("some", StringType(), True),
  StructField("col", StringType(), True),
  StructField("names", LongType(), False)
])

myRows = [Row("Hello", None, 1), 
          Row("Cruel", None, 2), 
          Row("World", None, 3)]

myDf = spark.createDataFrame(myRows, myManualSchema)
display(myDf)

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType

data = list(range(10))

myDf = spark.createDataFrame(data, LongType())

display(myDf)

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType

myManualSchema = StructType([
  StructField("some", StringType(), True),
  StructField("complex", StructType([
     StructField("col", StringType(), True),
     StructField("names", LongType(), False)   
  ]),  True)
])

myRows = [Row("Hello", Row(None, 1)),
          Row("Cruel", Row(None, 2)), 
          Row("World", Row(None, 3))]

myDf = spark.createDataFrame(myRows, myManualSchema)
display(myDf)

# COMMAND ----------

print(myDf.first()[1][1])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # From Dictionary

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

schema = StructType([
  StructField("measure", StringType(), True),
  StructField("value", IntegerType(), True)
])

stats = {
  "measure1": 1000,
  "measure2": 1000
}

rows = [Row(key, value) for (key,value) in stats.items()]
df = spark.createDataFrame(rows, schema)
df.show()

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

col_1 = "measure"
col_2 = "value"

schema = StructType([
  StructField(col_1, StringType(), True),
  StructField(col_2, IntegerType(), True)
])

stats = {
  "validRows": 1000,
  "expectedRows": 1000
}

rows = [{col_1:key, col_2: value} for (key,value) in stats.items()]
df = spark.createDataFrame(rows, schema)
display(df)

# COMMAND ----------

# without a pivot
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

col_1 = "measure"
col_2 = "value"

schema = StructType([
  StructField(col_1, StringType(), True),
  StructField(col_2, IntegerType(), True)
])

# transform dictionary
rows = [{col_1:key, col_2: value} for (key,value) in stats.items()]
df = spark.createDataFrame(rows, schema)
display(df)
