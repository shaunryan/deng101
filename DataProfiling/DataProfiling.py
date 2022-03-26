# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Stage Flights Data
# MAGIC 
# MAGIC To perform these DataFrame operations, we’ll first prepare some data. In the following code snippet, we:
# MAGIC 
# MAGIC Import two files and create two DataFrames, one for airport (airportsna) information and one for US flight delays (departureDelays).
# MAGIC 
# MAGIC Using expr(), convert the delay and distance columns from STRING to INT.
# MAGIC 
# MAGIC Create a smaller table, foo, that we can focus on for our demo examples; it contains only information on three flights originating from Seattle (SEA) to the destination of San Francisco (SFO) for a small time range.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Airports

# COMMAND ----------

# MAGIC %pip install pandas-profiling

# COMMAND ----------

# In Python
# Set file paths
from pyspark.sql.functions import expr
tripdelaysFilePath = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
airportsnaFilePath = "/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"
  
# Obtain airports data set
airportsna = (spark.read
  .format("csv")
  .options(header="true", inferSchema="true", sep="\t")
  .load(airportsnaFilePath))

airportsna.createOrReplaceTempView("airports_na")
display(airportsna)


# COMMAND ----------

import pandas as pd
from pandas_profiling import ProfileReport

profile = ProfileReport(airportsna.toPandas(), title="Pandas Profiling Report", explorative=True)
displayHTML(profile.to_html())

# COMMAND ----------

# https://stackoverflow.com/questions/52553062/pandas-profiling-doesnt-display-the-output 
import pandas as pd
from sklearn.datasets import fetch_california_housing
from pandas_profiling import ProfileReport

california_housing = fetch_california_housing()

pd_df_california_housing = pd.DataFrame(california_housing.data, columns = california_housing.feature_names) 
pd_df_california_housing['target'] = pd.Series(california_housing.target)

profile = ProfileReport(pd_df_california_housing, title="Pandas Profiling Report", explorative=True)
displayHTML(profile.to_html())

# COMMAND ----------

df = dbutils.data.summarize(airportsna)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Departure Delays

# COMMAND ----------

# Obtain departure delays data set
departureDelays = (spark.read
  .format("csv")
  .options(header="true")
  .load(tripdelaysFilePath))

departureDelays = (departureDelays
  .withColumn("delay", expr("CAST(delay as INT) as delay"))
  .withColumn("distance", expr("CAST(distance as INT) as distance")))

departureDelays.createOrReplaceTempView("departureDelays")
display(departureDelays)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Departure Delays
# MAGIC ```
# MAGIC origin == 'SEA' 
# MAGIC and destination == 'SFO' 
# MAGIC and date like '01010%' and delay > 0
# MAGIC ```

# COMMAND ----------

# Create temporary small table
foo = (departureDelays
  .filter(expr("""origin == 'SEA' and destination == 'SFO' and 
    date like '01010%' and delay > 0""")))
foo.createOrReplaceTempView("foo")

display(foo)
