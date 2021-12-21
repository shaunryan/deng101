# Databricks notebook source
# MAGIC %md
# MAGIC ## Great Expectations
# MAGIC A simple demonstration of how to use the basic functions of the Great Expectations library with Pyspark

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install great_expectations

# COMMAND ----------

import great_expectations as ge
import pandas as pd

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType(
  [StructField("String", StringType()),
   StructField("Value", StringType())]
)

tuple = (["one", 1], ["two", 2], ["two", 2])
df = spark.createDataFrame(t_list, schema)
df.createOrReplaceTempView("tC_python")


# COMMAND ----------

# first lets create a simple dataframe

data = {
  "String": ["one", "two", "two",],
  "Value": [1, 2, 2,],
}

# lets create a pandas dataframe
pd_df = pd.DataFrame.from_dict(data)

# we can use pandas to avoid needing to define schema
df = spark.createDataFrame(
  pd_df
)

display(df)

# COMMAND ----------

# now let us create the appropriate great-expectations objects

# for pandas we create a great expectations object like this
pd_df_ge = ge.from_pandas(pd_df)

# while for pyspark we can do it like this
df_ge = ge.dataset.SparkDFDataset(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Running Great Expectations tests
# MAGIC 
# MAGIC Expectations return a dictionary of metadata, including a boolean "success" value

# COMMAND ----------

#this works the same for bot Panmdas and PySpark Great Expectations datasets
print(pd_df_ge.expect_table_row_count_to_be_between(1,10))

print(df_ge.expect_table_row_count_to_be_between(1,10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Differences between Great Expectations Pandas and Pyspark Datasets

# COMMAND ----------

# pandas datasets inherit all the pandas dataframe methods
print(pd_df_ge.count())

# while GE pyspark datasets do not and the following leads to an error
print(df_ge.count())

# COMMAND ----------

# however you can access the original pyspark dataframe using df_ge.spark_df

df_ge.spark_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Taking Great Expectations further
# MAGIC 
# MAGIC If you want to make use of Great Expectations data context features you will need to install a data context. details can be found here https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_data_contexts/how_to_instantiate_a_data_context_on_a_databricks_spark_cluster.html  

# COMMAND ----------

print(df_ge)

# COMMAND ----------

df_ge.count()

# COMMAND ----------

pd_df_ge.count()

# COMMAND ----------

pd_df.dataframe.count()
