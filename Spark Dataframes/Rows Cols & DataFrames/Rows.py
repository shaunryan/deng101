# Databricks notebook source
# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.Row
# MAGIC val myRow = Row("Hello", null, 1, false)
# MAGIC 
# MAGIC println(myRow(0))
# MAGIC println(myRow(0).asInstanceOf[String])
# MAGIC println(myRow.getString(0))
# MAGIC println(myRow.getInt(2))

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql import Row
# MAGIC myRow = Row("Hello", None, 1, False)
# MAGIC 
# MAGIC print(myRow[0])
# MAGIC print(myRow[1])
# MAGIC print(myRow[2])
# MAGIC print(myRow[3])

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val df = spark.read.format("json")
# MAGIC .load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json")
# MAGIC 
# MAGIC val firstRow = df.first()
# MAGIC 
# MAGIC println(firstRow.getAs[String]("DEST_COUNTRY_NAME"))

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC df = spark.read.format("json") \
# MAGIC .load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json")
# MAGIC 
# MAGIC firstRow = df.first()
# MAGIC 
# MAGIC print(firstRow["DEST_COUNTRY_NAME"])
# MAGIC print(firstRow[0])
# MAGIC print(firstRow.DEST_COUNTRY_NAME)

# COMMAND ----------

display(df.select(*_))
