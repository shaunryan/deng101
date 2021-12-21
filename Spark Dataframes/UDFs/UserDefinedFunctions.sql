-- Databricks notebook source
-- MAGIC %scala
-- MAGIC 
-- MAGIC import org.apache.spark.sql.functions.udf
-- MAGIC 
-- MAGIC val parseDouble = (s: String) =>
-- MAGIC {
-- MAGIC   try { Some(s.toDouble) }
-- MAGIC   catch { case nfe : java.lang.NumberFormatException => None }
-- MAGIC }
-- MAGIC spark.udf.register("parseDouble", parseDouble)

-- COMMAND ----------

drop table if exists TestToDouble;
Create table TestToDouble
(
stringDouble string
)
using delta

-- COMMAND ----------

drop table if exists TestDoneToDouble;
Create table TestDoneToDouble
(
stringDouble Decimal(18,9)
)
using delta

-- COMMAND ----------

insert into TestToDouble 
values
("10.49634576934857693846"),
("`")

-- COMMAND ----------

insert into TestDoneToDouble
select parseDouble(stringDouble)
from TestToDouble

-- COMMAND ----------

select * from TestDoneToDouble

-- COMMAND ----------

select double(stringDouble) from TestToDouble
