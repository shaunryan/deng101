# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Higher Order Functions
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Because complex data types are amalgamations of simple data types, it is tempting to manipulate them directly. There are two [typical solutions](https://databricks.com/blog/2018/11/16/introducing-new-built-in-functions-and-higher-order-functions-for-complex-data-types-in-apache-spark.html) for manipulating complex data types:
# MAGIC 
# MAGIC Exploding the nested structure into individual rows, applying some function, and then re-creating the nested structure
# MAGIC 
# MAGIC Building a user-defined function
# MAGIC 
# MAGIC These approaches have the benefit of allowing you to think of the problem in tabular format. They typically involve (but are not limited to) using [utility functions](https://databricks.com/blog/2018/11/16/introducing-new-built-in-functions-and-higher-order-functions-for-complex-data-types-in-apache-spark.html) such as:
# MAGIC ```
# MAGIC get_json_object()
# MAGIC from_json()
# MAGIC to_json()
# MAGIC explode()
# MAGIC selectExpr
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Explode & Collect

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC - While `collect_list()`` returns a list of objects with duplicates
# MAGIC - the `GROUP BY` statement requires shuffle operations, meaning the order of the re-collected array isn’t necessarily the same as that of the original array. 
# MAGIC - As values could be any number of dimensions (a really wide and/or really long array) and we’re doing a `GROUP BY`, this approach could be very expensive.
# MAGIC - `collect_list()` may cause executors to OOME for large data sets

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- adds 1 to each element by exploding out, transforming and collecting
# MAGIC 
# MAGIC -- collect_list() returns a list of objects with duplicates
# MAGIC SELECT id, collect_list(value + 1) as values
# MAGIC FROM (
# MAGIC   -- creates a new row for each value in the list
# MAGIC   SELECT id, EXPLODE(values) as value
# MAGIC   FROM table
# MAGIC ) x
# MAGIC GROUP BY id

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## User-Defined Function
# MAGIC 
# MAGIC To perform the same task, we can also create a UDF that uses map() to iterate through each element (value) and perform the addition operation
# MAGIC 
# MAGIC This is better since there won't be any re-ordering or OOME issues. The serialization and de-serialization process could be expensive.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def addOne(values: Seq[Int]): Seq[Int] = {
# MAGIC   values.map(value => value + 1)
# MAGIC }
# MAGIC val plusOneInt = spark.udf.register("plusOneInt", addOne(_: Seq[Int]): Seq[Int])

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- can then be used in SQL
# MAGIC SELECT id, plusOneInt(values) as values FROM table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Built-In Functions for Complex Data Types
# MAGIC 
# MAGIC Instead of using these potentially expensive techniques, you may be able to use some of the built-in functions for complex data types.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC    (SELECT array_distinct(array(1, 2, 3, null, 3))) as array_distinct          -- 
# MAGIC   ,(SELECT array_intersect(array(1, 2, 3), array(1, 3, 5))) as array_intersect -- 
# MAGIC   ,(SELECT array_union(array(1, 2, 3), array(1, 3, 5))) as array_union         -- 
# MAGIC   ,(SELECT array_except(array(1, 2, 3), array(1, 3, 5))) as array_except       --
# MAGIC   ,(SELECT array_join(array('hello', 'world'), ' ')) as array_join             --
# MAGIC   ,(SELECT array_max(array(1, 20, null, 3))) as array_max                      -- 
# MAGIC   ,(SELECT array_min(array(1, 20, null, 3))) as array_min                      -- 
# MAGIC   ,(SELECT array_position(array(3, 2, 1), 1)) as array_position                -- 
# MAGIC   ,(SELECT array_remove(array(1, 2, 3, null, 3), 3)) as array_remove           -- 
# MAGIC   ,(SELECT arrays_overlap(array(1, 2, 3), array(3, 4, 5))) as arrays_overlap   -- 
# MAGIC   ,(SELECT array_sort(array('b', 'd', null, 'c', 'a'))) as array_sort          -- 
# MAGIC   ,(SELECT concat(array(1, 2, 3), array(4, 5), array(6))) as concat            -- 
# MAGIC   ,(SELECT flatten(array(array(1, 2), array(3, 4)))) as flatten                -- 
# MAGIC   ,(SELECT array_repeat('123', 3)) as array_repeat                             -- 
# MAGIC   ,(SELECT reverse(array(2, 1, 4, 3))) as reverse                              -- 
# MAGIC   ,(SELECT sequence(1, 5)) as sequence_asc                                     -- 
# MAGIC   ,(SELECT sequence(5, 1)) as sequence_desc                                    -- 
# MAGIC   ,(SELECT sequence(to_date('2018-01-01'), to_date('2018-03-01')
# MAGIC            , interval 1 month)) as sequence_months                             -- 
# MAGIC   ,(SELECT shuffle(array(1, 20, null, 3))) as shuffle                          -- 
# MAGIC   ,(SELECT slice(array(1, 2, 3, 4), -2, 2)) as slice                           -- 
# MAGIC   ,(SELECT arrays_zip(array(1, 2), array(2, 3), array(3, 4))) as array_zip     -- Returns a merged array of structs
# MAGIC   ,(SELECT element_at(array(1, 2, 3), 2)) as element_at                        -- 
# MAGIC   ,(SELECT cardinality(array('b', 'd', 'c', 'a'))) as cardinality              -- size
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT map_from_arrays(array(1.0, 3.0), array('2', '4'))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC    (SELECT map_from_arrays(array(1.0, 3.0), array('2', '4')))       as map_from_arrays   -- 
# MAGIC   ,(SELECT map_from_entries(array(struct(1, 'a'), struct(2, 'b')))) as map_from_entries  -- 
# MAGIC   ,(SELECT map_concat(map(1, 'a', 2, 'b'), map(3, 'c', 4, 'd')))    as map_concat        -- 
# MAGIC   ,(SELECT element_at(map(1, 'a', 2, 'b'), 2))                      as element_at        -- 
# MAGIC   ,(SELECT cardinality(map(1, 'a', 2, 'b')))                        as cardinality       -- 
# MAGIC ;
