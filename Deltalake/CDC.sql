-- Databricks notebook source
drop table `TestDBSql1`.`TestTable1`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('abfss://datalake@datalake.dfs.core.windows.net/databricks/delta/testdb', True)

-- COMMAND ----------

CREATE OR REPLACE TABLE `TestDBSql1`.`TestTable1` (
  `UUID` STRING,
  `Id` BIGINT,
  `Data` STRING,
  `Type` STRING,
  `Time` TIMESTAMP
)
USING delta
LOCATION 'abfss://datalake@datalake.dfs.core.windows.net/databricks/delta/testdb/TestTable1'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- COMMAND ----------

describe table extended `TestDBSql1`.`TestTable1`

-- COMMAND ----------

insert into `TestDBSql1`.`TestTable1`
select uuid(), 1, "hello", "geeting", now() union all
select uuid(), 1, "kind", "noun", now() union all
select uuid(), 1, "world", "planet", now()


-- COMMAND ----------

SELECT * FROM table_changes('TestDBSql1.TestTable1', 0, 10)

-- COMMAND ----------

DESCRIBE HISTORY `TestDBSql1`.`TestTable1`;

-- COMMAND ----------

select * from  `TestDBSql1`.`TestTable1`;
