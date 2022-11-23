-- Databricks notebook source
CREATE WIDGET TEXT slice_date DEFAULT "2020/01/01"

-- COMMAND ----------

select coalesce(to_date(GetArgument ("slice_date"), 'y/M/D'), CURRENT_DATE()) as CurrentDate

-- COMMAND ----------

select coalesce(to_timestamp(GetArgument ("slice_date"), 'y/M/D'), CURRENT_TIMESTAMP()) as CurrentTimestamp

-- COMMAND ----------

DROP WDIGET database
