# Databricks notebook source
broken_schema_txt = """category,subcategory,size
"widget A","slider 1",2
"widget A","slider 2",4
"widget C\\","slider 1",3
"widget D","slider 1",5
"widget E","slider 1",7
"""

filename = "broken_schema.csv"

tmp_path = f"/tmp/{filename}"
path = f"/FileStore/{filename}"

with open(tmp_path, "w") as file:
  file.write(broken_schema_txt)

dbutils.fs.cp(f"file:{tmp_path}", path)


# COMMAND ----------


from pyspark.sql.types import ArrayType,StructField,StructType,IntegerType,StringType

schema = StructType([
    StructField("category",StringType(),True),
    StructField("subcategory",StringType(),True),
    StructField("size",IntegerType(),True),
#     StructField("_corrupt_record", StringType(), True)
])

# COMMAND ----------

from pyspark.sql.functions import col

options = {
  "header": True,
  "delimiter": ',',
  "escape": '\\',
  "badRecordsPath": "/FileStore/broken_schema_badRecords"
}
df = (spark
      .read
      .format("csv")
      .options(**options)
      .schema(schema)
      .load(path))

# invalid_count = df.count()
df.cache()
# valid_count = df.count()


# COMMAND ----------

df.explain()

# COMMAND ----------

display(df)

# COMMAND ----------

spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", False)
spark.conf.get("spark.sql.csv.parser.columnPruning.enabled")

# COMMAND ----------

df1 = df.select("category")
display(df1)

# COMMAND ----------

df.cache()
df.count()
