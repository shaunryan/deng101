// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # Common DataFrames & Spark SQL Operations
// MAGIC 
// MAGIC [Operations](https://spark.apache.org/docs/latest/api/sql/index.html):
// MAGIC - Aggregate functions
// MAGIC - Collection functions
// MAGIC - Datetime functions
// MAGIC - Math functions
// MAGIC - Miscellaneous functions
// MAGIC - Non-aggregate functions
// MAGIC - Sorting functions
// MAGIC - String functions
// MAGIC - UDF functions
// MAGIC - Window functions
// MAGIC 
// MAGIC Relational Operations:
// MAGIC - Unions
// MAGIC - Joins
// MAGIC - Windowing
// MAGIC - Modifications

// COMMAND ----------

// MAGIC %run ../Datasets/pyFlights

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Unions

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC bar = departureDelays.union(foo)
// MAGIC 
// MAGIC bar.createOrReplaceTempView("bar")
// MAGIC bar = bar.filter(expr("""
// MAGIC origin == 'SEA' AND
// MAGIC destination == 'SFO' AND
// MAGIC date LIKE '01010%' AND
// MAGIC delay > 0
// MAGIC """))
// MAGIC 
// MAGIC display(bar)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Joins
// MAGIC 
// MAGIC A common DataFrame operation is to [join](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.join.html?highlight=pyspark%20sql%20dataframe%20join) 2 DataFrames tables together:
// MAGIC 
// MAGIC - inner
// MAGIC - inner
// MAGIC - cross
// MAGIC - outer
// MAGIC - full
// MAGIC - fullouter
// MAGIC - full_outer
// MAGIC - left
// MAGIC - leftouter
// MAGIC - left_outer
// MAGIC - right
// MAGIC - rightouter
// MAGIC - right_outer
// MAGIC - semi
// MAGIC - leftsemi
// MAGIC - left_semi
// MAGIC - anti
// MAGIC - leftanti
// MAGIC - left_anti

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC cols  = ["City", "State", "date", "delay", "distance", "destination"]
// MAGIC 
// MAGIC joined = foo.join(
// MAGIC   airportsna, airportsna.IATA == foo.origin
// MAGIC ).select(cols)
// MAGIC 
// MAGIC display(joined)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Windowing
// MAGIC https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html
// MAGIC https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Window.html?highlight=windowing
// MAGIC 
// MAGIC Operate on a group of rows but returning a value for each single row. Window Fuctions
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC |                 SQL | DataFrame              | API               |
// MAGIC |---------------------|------------------------|-------------------|
// MAGIC |`Ranking functions`  | `rank()`	           | `rank()`          |
// MAGIC |                     | `dense_rank()`	       | `denseRank()`     |
// MAGIC |                     | `percent_rank()`	   | `percentRank()`   |
// MAGIC |                     | `ntile()`	           | `ntile()`         |
// MAGIC |                     | `row_number()`	       | `rowNumber()`     | 
// MAGIC |`Analytic functions` | `cume_dist()`		   | `cumeDist()`      |
// MAGIC |                     | `first_value()`	       | `firstValue()`    |
// MAGIC |                     | `last_value()`	       | `lastValue()`     |
// MAGIC |                     | `lag()`		           | `lag()`           |
// MAGIC |                     | `lead()`		       | `lead()`          |

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DROP TABLE IF EXISTS departureDelaysWindow;
// MAGIC 
// MAGIC CREATE TABLE departureDelaysWindow AS
// MAGIC 
// MAGIC SELECT 
// MAGIC   origin, 
// MAGIC   destination, 
// MAGIC   SUM(delay) as TotalDelays
// MAGIC FROM departureDelays
// MAGIC WHERE origin IN ('SEA', 'SFO', 'JFK')
// MAGIC   AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
// MAGIC GROUP BY origin, destination;
// MAGIC 
// MAGIC SELECT * FROM departureDelaysWindow;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC What if for each of these origin airports you wanted to find the three destinations that experienced the most delays

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT 
// MAGIC   origin, 
// MAGIC   destination, 
// MAGIC   TotalDelays, 
// MAGIC   rank
// MAGIC FROM (
// MAGIC 
// MAGIC   SELECT 
// MAGIC     origin, 
// MAGIC     destination, 
// MAGIC     TotalDelays, 
// MAGIC     dense_rank() OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank
// MAGIC   FROM departureDelaysWindow
// MAGIC 
// MAGIC ) t
// MAGIC WHERE rank <= 3

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql import Window
// MAGIC import pyspark.sql.functions as func
// MAGIC import sys
// MAGIC 
// MAGIC # ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
// MAGIC # window = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
// MAGIC 
// MAGIC # PARTITION BY country ORDER BY date RANGE BETWEEN 3 PRECEDING AND 3 FOLLOWING
// MAGIC # window = Window.orderBy("date").partitionBy("country").rangeBetween(-3, 3)
// MAGIC 
// MAGIC windowSpec = Window \
// MAGIC   .orderBy("TotalDelays") \
// MAGIC   .partitionBy("origin") 
// MAGIC 
// MAGIC win_col = func.dense_rank().over(windowSpec)
// MAGIC 
// MAGIC   
// MAGIC df = spark.sql("""
// MAGIC SELECT 
// MAGIC   origin, 
// MAGIC   destination, 
// MAGIC   SUM(delay) as TotalDelays
// MAGIC FROM departureDelays
// MAGIC WHERE origin IN ('SEA', 'SFO', 'JFK')
// MAGIC   AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
// MAGIC GROUP BY origin, destination
// MAGIC """)
// MAGIC 
// MAGIC 
// MAGIC cols = df.columns
// MAGIC df = df.withColumn("rank", win_col) \
// MAGIC   .where("rank <= 3") \
// MAGIC   .select(cols)
// MAGIC 
// MAGIC display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Modifications

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC display(foo)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC # In Python
// MAGIC 
// MAGIC 
// MAGIC from pyspark.sql.functions import expr
// MAGIC foo2 = foo \
// MAGIC   .withColumn("status", expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")) \
// MAGIC   .drop("delay") \
// MAGIC   .withColumnRenamed("status", "flight_status")
// MAGIC 
// MAGIC display(foo2)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Pivoting
// MAGIC 
// MAGIC https://databricks.com/blog/2018/11/01/sql-pivot-converting-rows-to-columns.html
// MAGIC 
// MAGIC Swapping columns for rows

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select
// MAGIC   destination,
// MAGIC   cast(substring(date, 0, 2) as int) as month, delay
// MAGIC from departureDelays
// MAGIC where origin = 'SEA'

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from (
// MAGIC   select 
// MAGIC     destination, 
// MAGIC     cast(substring(date, 0, 2) as int) as month, 
// MAGIC     delay
// MAGIC   from departureDelays where origin = 'SEA'
// MAGIC )
// MAGIC pivot (
// MAGIC   cast (avg(delay) as decimal(4, 2)) as AvgDelay,
// MAGIC   max(delay) as MaxDelay
// MAGIC   for month in (1 Jan, 2 Feb)
// MAGIC )
// MAGIC order by destination
