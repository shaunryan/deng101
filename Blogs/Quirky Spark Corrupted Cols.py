# Databricks notebook source
# MAGIC %scala
# MAGIC // dbutils.notebook.getContext.tags("sparkVersion")
# MAGIC // val version = dbutils.notebook.getContext.tags("sparkVersion")
# MAGIC // print(version)
# MAGIC // assert (version == "10.4.x-scala2.12")

# COMMAND ----------

# assert sc.version == '3.2.1';


# COMMAND ----------

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


from pyspark.sql.types import (
  StructField,StructType,IntegerType,StringType
)

schema = StructType([
    StructField("category",StringType(),True),
    StructField("subcategory",StringType(),True),
    StructField("size",IntegerType(),True),
    StructField("_corrupt_record", StringType(), True)
])

# COMMAND ----------

from pyspark.sql.functions import col

options = {
  "header": True,
  "mode": "PERMISSIVE",
  "delimiter": ',',
  "escape": '\\',
  "columnNameOfCorruptRecord": "_corrupt_record" # this is the dafault
}
df = (spark
      .read
      .format("csv")
      .options(**options)
      .schema(schema)
      .load(path)
      .filter(col("_corrupt_record").isNotNull()))

display(df)



# COMMAND ----------

from pyspark.sql.functions import col

options = {
  "header": True,
  "mode": "PERMISSIVE",
  "delimiter": ',',
  "escape": '\\',
  "columnNameOfCorruptRecord": "_corrupt_record" # this is the dafault
}
df = (spark
      .read
      .format("csv")
      .options(**options)
      .schema(schema)
      .load(path))

df_fixed = df.filter(col("_corrupt_record").isNotNull())

display(df)
