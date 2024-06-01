# Databricks notebook source
import discover_modules
discover_modules.go(spark)

# COMMAND ----------

from utilities import AppConfig

app_config = AppConfig(dbutils, spark)
app_config.help()

# COMMAND ----------

app_config.connect_storage()
display(dbutils.fs.ls(app_config.get_storage_account()))

# COMMAND ----------


tables = ['dimaccount', 'dimchannel', 'dimcurrency', 'dimcustomer', 'dimdate', 'dimemployee', 'dimentity', 'dimgeography', 'dimmachine', 'dimoutage', 'dimproduct', 'dimproductcategory', 'dimproductsubcategory', 'dimpromotion', 'dimsalesterritory', 'dimscenario', 'dimstore']

for t in tables:
  
  df = spark.sql(
    f"""
    select s._partition, p.*
    from contosoretaildw.{t} p
    cross join (
      select 
        distinct cast(date_format(DateKey, 'yMM') as INT) as `_partition`
      from contosoretaildw.factsales
    ) s
    """).repartition(1)


  df.write \
    .partitionBy("_partition") \
    .mode("overwrite") \
    .json(f"/mnt/datalake/source/contosoretaildw/{t}")


# COMMAND ----------

# fact tables

tables = ['factexchangerate', 'factinventory', 'factitmachine', 'factitsla', 'factonlinesales', 'factsales', 'factsalesquota', 'factstrategyplan']

for t in tables:
  df = spark.sql(
    f"""
    select 
      distinct cast(date_format(DateKey, 'yMM') as INT) as `_partition`, *
    from contosoretaildw.{t}
    """).repartition(1)


  df.write \
    .partitionBy("_partition") \
    .mode("overwrite") \
    .json(f"/mnt/datalake/source/contosoretaildw/{t}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from contosoretaildw.factsales
# MAGIC -- product
# MAGIC -- promotion
# MAGIC -- sales key
# MAGIC -- store key
# MAGIC -- channel key
