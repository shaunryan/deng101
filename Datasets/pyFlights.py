# Databricks notebook source
# MAGIC %pip install pandas-profiling

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Pandas Profiling

# COMMAND ----------

# In Python
# Set file paths
from pyspark.sql.functions import expr
import pandas as pd
from pandas_profiling import ProfileReport

tripdelaysFilePath = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
airportsnaFilePath = "/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"
  
# Obtain airports data set
airportsna = (spark.read
  .format("csv")
  .options(header="true", inferSchema="true", sep="\t")
  .load(airportsnaFilePath))

# airportsna.createOrReplaceTempView("airports_na")
# display(airportsna)

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
# displayHTML(profile.to_html())
profile.to_file('/tmp/profile_report.html')
dbutils.fs.ls("file:/tmp/")
dbutils.fs.cp("file:/tmp/profile_report.html", "/FileStore/data_profiles/profile_report.html")
displayHTML("""
<a href='https://adb-8723178682651460.0.azuredatabricks.net/files/data_profiles/profile_report.html'>report</a>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks DButils

# COMMAND ----------

df = dbutils.data.summarize(airportsna)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Koalas - unfortunately doesn't work!

# COMMAND ----------

import pyspark.pandas as pd
from sklearn.datasets import fetch_california_housing
from pandas_profiling import ProfileReport

california_housing = fetch_california_housing()

pd_df_california_housing = pd.DataFrame(california_housing.data, columns = california_housing.feature_names) 
type(pd_df_california_housing)
# profile = ProfileReport(pd_df_california_housing, title="Pandas Profiling Report", explorative=True)
# profile.to_file('/tmp/profile_report.html')
# displayHTML(profile.to_html())
# profile.to_file('/tmp/profile_report.html')
# dbutils.fs.ls("file:/tmp/")
# dbutils.fs.cp("file:/tmp/profile_report.html", "/FileStore/data_profiles/profile_report.html")
# displayHTML("""
# <a href='https://adb-8723178682651460.0.azuredatabricks.net/files/data_profiles/profile_report.html'>report</a>
# """)
