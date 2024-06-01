# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType, TimestampType
from pyspark.sql.functions import to_timestamp, col, lit
from uuid import uuid4

myManualSchema = StructType([
  StructField("Id", StringType(), True),
  StructField("Firstname", StringType(), True),
  StructField("Surname", StringType(), True),
  StructField("CreatedDate", StringType(), False)
])

shaunId = str(uuid4())
finelyId = str(uuid4())
sarahId = str(uuid4())

myRows = [Row(shaunId,  "Shaun", "Ryan",  "2021-10-28 00:00:00"), 
          Row(shaunId,  "Shaun", "Ryan",  "2021-10-29 00:00:00"), 
          Row(shaunId,  "Shaun", "Ryan",  "2021-10-30 00:00:00"), 
          Row(shaunId,  "Shaun", "Ryan",  "2021-10-31 00:00:00"),
          Row(finelyId, "Finley", "Ryan", "2021-10-28 00:00:00"), 
          Row(finelyId, "Finley", "Ryan", "2021-10-29 00:00:00"), 
          Row(sarahId,  "Sarah", "Ryan",  "2021-11-01 00:00:00"), 
          Row(sarahId,  "Sarah", "Ryan",  "2021-11-02 00:00:00")]

myDf = spark.createDataFrame(myRows, myManualSchema)
myDf = myDf.withColumn("CreatedDate", to_timestamp("CreatedDate", "yyyy-MM-dd HH:mm:ss"))

myDf.createOrReplaceTempView("Type4")

myDf = spark.sql("select * from Type4")
display(myDf)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Get the latest.
# MAGIC
# MAGIC with cte_versioned as
# MAGIC (
# MAGIC   select
# MAGIC     row_number() over(
# MAGIC       partition by Id 
# MAGIC       order by CreatedDate desc
# MAGIC     ) as RowVersion,
# MAGIC     Id,
# MAGIC     Firstname,
# MAGIC     Surname,
# MAGIC     CreatedDate
# MAGIC   from Type4
# MAGIC )
# MAGIC
# MAGIC select
# MAGIC   Id,
# MAGIC   Firstname,
# MAGIC   Surname,
# MAGIC   CreatedDate
# MAGIC from cte_versioned 
# MAGIC where RowVersion = 1 

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC with cte_versioned as
# MAGIC (
# MAGIC   select
# MAGIC     lead(CreatedDate, 1, to_timestamp("2050-01-01 00:00:00.001")) 
# MAGIC       over(
# MAGIC         partition by Id 
# MAGIC         order by CreatedDate
# MAGIC       ) 
# MAGIC       - INTERVAL 1 MILLISECOND 
# MAGIC     as ToDate,
# MAGIC     
# MAGIC     (row_number() 
# MAGIC       over(
# MAGIC         partition by Id 
# MAGIC         order by CreatedDate desc
# MAGIC       )
# MAGIC     ) = 1 as IsCurrent,
# MAGIC     
# MAGIC     Id,
# MAGIC     Firstname,
# MAGIC     Surname,
# MAGIC     CreatedDate as FromDate
# MAGIC   from Type4
# MAGIC )
# MAGIC
# MAGIC select
# MAGIC   Id,
# MAGIC   Firstname,
# MAGIC   Surname,
# MAGIC   FromDate,
# MAGIC   ToDate,
# MAGIC   IsCurrent
# MAGIC from cte_versioned
# MAGIC order by id, FromDate
# MAGIC

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import col

rowVersionWindow  = Window.partitionBy("Id").orderBy(col("CreatedDate").desc())

myDf = (myDf
          .withColumn("RowVersion",row_number().over(rowVersionWindow))
          .where("RowVersion = 1")
       )

display(myDf)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, expr, lead, to_timestamp

rowVersionWindow  = Window.partitionBy("Id").orderBy(col("CreatedDate").desc())
toDateWindow  = Window.partitionBy("Id").orderBy(col("CreatedDate"))

df = (myDf
          .withColumn("RowVersion",row_number().over(rowVersionWindow))
          .withColumn("ToDate",lead("CreatedDate", 1, "2050-01-01 00:00:00.001").over(toDateWindow))
          .withColumn("ToDate",expr("ToDate - INTERVAL 1 MILLISECOND"))
          .withColumn("IsCurrent", expr("(RowVersion = 1)"))
          .withColumn("RowVersion",row_number().over(rowVersionWindow))
          .orderBy("id", "CreatedDate")
          .select("id", "Firstname", "Surname", "CreatedDate", "ToDate", "IsCurrent")
       )

display(df)
