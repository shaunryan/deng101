# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Best Practice
# MAGIC 
# MAGIC No guarantee of order of subexpressions
# MAGIC e.g
# MAGIC 
# MAGIC ```
# MAGIC spark.sql("SELECT s FROM test1 WHERE s IS NOT NULL AND strlen(s) > 1")
# MAGIC ```
# MAGIC 
# MAGIC There is no guarantee that strlen will recieve not null. 
# MAGIC 
# MAGIC Therefore:
# MAGIC - UDF's should be null aware with null checks
# MAGIC - Use if or case expression to null before invoking the UDF

# COMMAND ----------

# DBTITLE 1,Register a function as a UDF
from pyspark.sql.types import LongType

# Create a cubed function
def cubed(s):
  return s * s * s

spark.udf.register("cubed", cubed, LongType())

# Generate temporary view
spark.range(1, 9).createOrReplaceTempView("udf_test")

# COMMAND ----------

# DBTITLE 1,SQL UDF
df = spark.sql("SELECT id, cubed(id) as id_cubed FROM udf_test")
display(df)

# COMMAND ----------

# DBTITLE 1,Python Dataframe UDF
# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.functions import udf
# MAGIC from pyspark.sql.types import LongType
# MAGIC 
# MAGIC cubed_udf = udf(cubed, LongType())
# MAGIC 
# MAGIC df = spark.table("udf_test")
# MAGIC display(df.select("id", cubed_udf("id").alias("id_cubed")))

# COMMAND ----------

# DBTITLE 1,Python Dataframe Annotated UDF
from pyspark.sql.functions import udf

@udf("long")
def squared_udf(s):
  return s * s

df = spark.table("udf_test")
display(df.select("id", squared_udf("id").alias("id_squared")))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Speeding up Pyspark UDFS with Pandas
# MAGIC 
# MAGIC There's 2 catetgories:
# MAGIC 
# MAGIC ### Pandas UDFs
# MAGIC 
# MAGIC Pandas UDF type is inferred from hints in Pandas UDF's such as:
# MAGIC ```
# MAGIC pandas.series
# MAGIC pandas.DataFrame
# MAGIC Tuple
# MAGIC Iterator
# MAGIC ```
# MAGIC 
# MAGIC Previously each Pandas UDF type had to be defined and specified on the signiture. Currently supported inputs and outputs are:
# MAGIC ```
# MAGIC Series => Series
# MAGIC Iterator of Series => Iterator of Series
# MAGIC Iterator of Multiple Series => Iterator of Series
# MAGIC Series => Scalar
# MAGIC ```
# MAGIC 
# MAGIC ### Pandas Function APIs
# MAGIC 
# MAGIC Directly apply a local python function to a pyspark dataframe where both input and output are Pandas instances. Spark 3.0 supported Pandas Function API's are:
# MAGIC ```
# MAGIC grouped map
# MAGIC map
# MAGIC co-grouped map
# MAGIC ```

# COMMAND ----------

# This is a regular Pandas Function
import pandas as pd

from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType

# declare cubed function
def cubed(a: pd.Series) -> pd.Series:
  return a * a * a

# creates a pandas UDF
cubed_udf = pandas_udf(cubed, returnType=LongType())


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Just a pandas function

# COMMAND ----------

# create a pandas Series 
x = pd.Series([1,2,3])

# just using the pandas function
print(cubed(x))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### A spark pandas function

# COMMAND ----------

# create a spark data frame 
df = spark.range(1, 4)

# execute the pandas function as a Spark vectorized UDF
cdf = df.select("id", cubed_udf(col('id')))
display(cdf)
