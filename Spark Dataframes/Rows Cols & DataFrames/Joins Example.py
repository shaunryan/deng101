# Databricks notebook source
from pyspark.sql import Row
from datetime import datetime
from pyspark.sql.types import StructField, StructType, StringType, DateType, IntegerType

peopleSchema = StructType([
  StructField("Name",   StringType(),  True),
  StructField("Family", IntegerType(), True),
  StructField("dob",    DateType(),    False)
])
dateformat = "%d-%m-%Y"

rows = [
  Row("Shaun",  1, datetime.strptime('25-01-1977', dateformat)),
  Row("Sarah",  1, datetime.strptime('10-01-1981', dateformat)),
  Row("Finley", 1, datetime.strptime('30-10-2017', dateformat)),
  
  Row("Paul",   2, datetime.strptime('12-04-1976', dateformat)),
  Row("Simon",  2, datetime.strptime('24-05-1978', dateformat))
]

dfp = spark.createDataFrame(rows, peopleSchema)
display(dfp)

# COMMAND ----------

familySchema = StructType([
  StructField("Name",     StringType(),  True),
  StructField("Family",   IntegerType(), True),
  StructField("PostCode", StringType(),  False)
])
dateformat = "%d-%m-%Y"

rows = [
  Row("Ryan",      1, "BS15 9RH"),
  Row("Pieman",    1, "YO16 6RE")
]

dff = spark.createDataFrame(rows, familySchema)
display(dff)

# COMMAND ----------

# the join column problem
from pyspark.sql.functions import expr, col, column

join = "Family = Family"

dfj = dfp.join(dff, join, "inner").drop(dff.Family)
display(dfj.select("Family"))

# COMMAND ----------

# DBTITLE 1,Implied Join - not this automatically reduces join columns to a single column
from pyspark.sql.functions import expr, col, column

dfj = dfp.join(dff, "Family", "inner")
display(dfj)


# COMMAND ----------

# DBTITLE 1,Explicit Join
from pyspark.sql.functions import expr, col, column

join = dfp["Family"] == dff["Family"]

dfj = dfp.join(dff, join, "inner")
display(dfj)

# COMMAND ----------

# DBTITLE 1,Explicit Join with String - will fail because of column abiguity
# the join column problem
from pyspark.sql.functions import expr, col, column

join = "Family = Family"

dfj = dfp.join(dff, join, "inner")
display(dfj)

# COMMAND ----------

# DBTITLE 1,Explicit Join with Expression - will fail because of column abiguity
# the join column problem
from pyspark.sql.functions import expr, col, column

join = dfp["Family"] == dff["Family"]

dfj = dfp.join(dff, join, "inner")
display(dfj.select("Family"))

# COMMAND ----------

# DBTITLE 1,Explicit Join with Expression - have to drop the column manually
# the join column problem
from pyspark.sql.functions import expr, col, column

join = dfp.Family == dff.Family

dfj = dfp.join(dff, join, "inner").drop(dff.Family)
display(dfj.select("Family"))
