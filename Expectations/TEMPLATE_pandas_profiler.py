# Databricks notebook source
# MAGIC %pip install pandas-profiling

# COMMAND ----------

from pyspark.sql.functions import expr
import pandas as pd
from pandas_profiling import ProfileReport


# COMMAND ----------

import pandas as pd
from pandas_profiling import ProfileReport


################################ Customise ######################################################
# from pyspark.sql.types import *

# schema = schema = StructType([ \
#     StructField('ID' , IntegerType(),True), \
#     ?
#   ])


# df = (spark
#       .read
#       .format("csv")
#       .schema(schema)
#       .option("mode", "failfast")
#       .load("?"))
############################################################################################

display(df)


# COMMAND ----------

profile = ProfileReport(df.toPandas(), title="Pandas Profiling Report", explorative=True)
profile.to_file('/tmp/profile_report.html')
dbutils.fs.ls("file:/tmp/")
dbutils.fs.cp("file:/tmp/profile_report.html", "/FileStore/data_profiles/profile_report.html")
displayHTML("""
<a href='https://8723178682651460.0.azuredatabricks.net/files/data_profiles/profile_report.html'>report</a>
""")



# COMMAND ----------

displayHTML(profile.to_html())
