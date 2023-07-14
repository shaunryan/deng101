-- Databricks notebook source
CREATE OR REPLACE VIEW `default`.ExampleWithSliceDate AS

SELECT 'a' as Letter, 1 as Number, to_date('2022-11-23') as _SliceDate union
SELECT 'b' as Letter, 2 as Number, to_date('2022-11-23') as _SliceDate union
SELECT 'c' as Letter, 3 as Number, to_date('2022-11-23') as _SliceDate union
SELECT 'd' as Letter, 4 as Number, to_date('2022-11-23') as _SliceDate union
SELECT 'e' as Letter, 5 as Number, to_date('2022-11-23') as _SliceDate union
SELECT 'f' as Letter, 6 as Number, to_date('2022-11-23') as _SliceDate union
SELECT 'g' as Letter, 7 as Number, to_date('2022-11-24') as _SliceDate union
SELECT 'h' as Letter, 8 as Number, to_date('2022-11-24') as _SliceDate union
SELECT 'i' as Letter, 9 as Number, to_date('2022-11-24') as _SliceDate union
SELECT 'j' as Letter, 10 as Number, to_date('2022-11-24') as _SliceDate union
SELECT 'k' as Letter, 11 as Number, to_date('2022-11-24') as _SliceDate 

-- COMMAND ----------

CREATE OR REPLACE VIEW `default`.ExampleWithoutSliceDate AS

SELECT 'a' as Letter, 1 as Number union
SELECT 'b' as Letter, 2 as Number union
SELECT 'c' as Letter, 3 as Number union
SELECT 'd' as Letter, 4 as Number union
SELECT 'e' as Letter, 5 as Number union
SELECT 'f' as Letter, 6 as Number union
SELECT 'g' as Letter, 7 as Number union
SELECT 'h' as Letter, 8 as Number union
SELECT 'i' as Letter, 9 as Number union
SELECT 'j' as Letter, 10 as Number  union
SELECT 'k' as Letter, 11 as Number

-- COMMAND ----------

select * from default.ColumnStripping

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Then in the python notebook dynamically where clause it and strip the system fields with a _leading underscore

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import DataFrame
-- MAGIC
-- MAGIC from datetime import datetime
-- MAGIC slice_date = "2022/11/23"
-- MAGIC slice_date_dte = datetime.strptime(slice_date, "%Y/%m/%d")
-- MAGIC
-- MAGIC
-- MAGIC def where_slice_date(
-- MAGIC   df:DataFrame,
-- MAGIC   slice_date: datetime,
-- MAGIC   slice_date_column:str = "_SliceDate",
-- MAGIC   stip_system_cols:bool = True
-- MAGIC ):
-- MAGIC   slice_date_str:str = datetime.strftime(slice_date, "%Y-%m-%d")
-- MAGIC   if slice_date_column in df.columns:
-- MAGIC     sql_where = f"`{slice_date_column}`=to_date('{slice_date_str}','yyyy-MM-dd')"
-- MAGIC     print(f"applying where clause: {sql_where}")
-- MAGIC     df = df.where(sql_where)
-- MAGIC
-- MAGIC   if stip_system_cols:
-- MAGIC     columns = [c for c in df.columns if not c.startswith("_")]
-- MAGIC     df = df.select(*columns)
-- MAGIC
-- MAGIC     return df
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # we can just where clause on the slice date 
-- MAGIC # if it exists and strip it from the return generically
-- MAGIC df = spark.sql("select * from default.ExampleWithSliceDate")
-- MAGIC df = where_slice_date(df, slice_date_dte)
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC # still works on views that don't have that column
-- MAGIC df = spark.sql("select * from default.ExampleWithoutSliceDate")
-- MAGIC df = where_slice_date(df, slice_date_dte)
-- MAGIC display(df)
