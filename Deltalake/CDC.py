# Databricks notebook source
# MAGIC %python
# MAGIC 
# MAGIC table_name = "name_job"
# MAGIC path = "/delta/default/cdc_demo"

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql import Row
# MAGIC from pyspark.sql.types import StructField, StructType, StringType, LongType
# MAGIC 
# MAGIC schema = StructType([
# MAGIC   StructField("name", StringType(), True),
# MAGIC   StructField("job", StringType(), True),
# MAGIC ])
# MAGIC 
# MAGIC rows = [{"name": "Rob", "job":"sweeper"},
# MAGIC           {"name": "Ben", "job":"cook"},
# MAGIC           {"name": "Jen", "job":"gardener"}]
# MAGIC 
# MAGIC df = spark.createDataFrame(rows, schema)
# MAGIC df.write.format("delta").mode("overwrite").saveAsTable(table_name, path=path)

# COMMAND ----------

# MAGIC %python
# MAGIC df = (spark.read.format("delta") 
# MAGIC     .option("readChangeFeed", "true") 
# MAGIC     .option("startingVersion", 0) 
# MAGIC     .table(table_name))
# MAGIC 
# MAGIC display(df)

# COMMAND ----------

spark.sql(f"update {table_name} set job = 'washer' where name = 'Ben'")

# COMMAND ----------

# MAGIC %python
# MAGIC df = (spark.read.format("delta") 
# MAGIC     .option("readChangeFeed", "true") 
# MAGIC     .option("startingVersion", 0) 
# MAGIC     .table(table_name))
# MAGIC 
# MAGIC display(df)

# COMMAND ----------

# MAGIC %python
# MAGIC df = (spark.readStream.format("delta")
# MAGIC      .option("readChangeFeed", "true")
# MAGIC #     .option("startingVersion", 0)
# MAGIC      .table(table_name))
# MAGIC 
# MAGIC display(df)

# COMMAND ----------

# MAGIC %python
# MAGIC df = spark.sql(f"SHOW TBLPROPERTIES {table_name}")
# MAGIC display(df)

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES(delta.enableChangeDataFeed = false)")
# MAGIC df = spark.sql(f"SHOW TBLPROPERTIES {table_name}")
# MAGIC display(df)

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES(delta.enableChangeDataFeed = true)")
# MAGIC df = spark.sql(f"SHOW TBLPROPERTIES {table_name}")
# MAGIC display(df)

# COMMAND ----------

# MAGIC %python
# MAGIC df = (spark.read.format("delta") 
# MAGIC     .option("readChangeFeed", "true") 
# MAGIC     .option("startingVersion", 0) 
# MAGIC     .table(table_name))
# MAGIC 
# MAGIC display(df)

# COMMAND ----------

# MAGIC %python
# MAGIC df = (spark.read.format("delta") 
# MAGIC #     .option("readChangeFeed", "true") 
# MAGIC #     .option("startingVersion", 0) 
# MAGIC     .table(table_name))
# MAGIC 
# MAGIC display(df)

# COMMAND ----------

dbutils.fs.rm(path, True)
spark.sql(f"drop table if exists {table_name}")
