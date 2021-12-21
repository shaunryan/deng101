# Databricks notebook source
# MAGIC %python
# MAGIC from pyspark.sql import Row
# MAGIC from pyspark.sql.types import StructField, StructType, StringType, LongType
# MAGIC 
# MAGIC schema = StructType([
# MAGIC   StructField("some", StringType(), True),
# MAGIC   StructField("col", StringType(), True),
# MAGIC   StructField("names", LongType(), False)
# MAGIC ])
# MAGIC 
# MAGIC rows = [Row("One", "A", 1), 
# MAGIC           Row("Two", "B", 2), 
# MAGIC           Row("Three", "C", 3)]
# MAGIC 
# MAGIC src_df = spark.createDataFrame(rows, schema)
# MAGIC display(src_df)

# COMMAND ----------

(src_df.write
  .format("delta")
  .option("mergeSchema", "true")
  .mode("append")
  .save("/delta/mergeschema/"))

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql import Row
# MAGIC from pyspark.sql.types import StructField, StructType, StringType, LongType
# MAGIC 
# MAGIC schema = StructType([
# MAGIC   StructField("some", StringType(), True),
# MAGIC #   StructField("col", StringType(), True),
# MAGIC   StructField("ohno", StringType(), True),
# MAGIC   StructField("names", LongType(), False)
# MAGIC ])
# MAGIC 
# MAGIC rows = [Row("One", "tada", 1), 
# MAGIC         Row("Two", "check", 2), 
# MAGIC         Row("Three", "me", 3)]
# MAGIC 
# MAGIC src_df = spark.createDataFrame(rows, schema)
# MAGIC display(src_df)

# COMMAND ----------

dest_df = spark.read.format("delta").load("/delta/mergeschema/").where("1=0")

deleted_cols = [c for c in dest_df.columns if c not in src_df.columns]
new_columns = [c for c in src_df.columns if c not in dest_df.columns]

for d in deleted_cols:
  src_df = src_df.withColumn(d, lit(None))

reordered_cols = dest_fd.columns + new_columns
src_df = src_df.select(*reordered_cols)


# COMMAND ----------

(df.write
  .format("delta")
  .option("mergeSchema", "true")
  .mode("append")
  .save("/delta/mergeschema/"))
