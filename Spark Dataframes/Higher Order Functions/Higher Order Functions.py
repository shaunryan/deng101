# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Higher Order Functions
# MAGIC 
# MAGIC - Take anonymous lambda functions as arguments
# MAGIC - Similar to the UDF approach, but more efficient
# MAGIC 
# MAGIC Functions:
# MAGIC 
# MAGIC ```
# MAGIC transform   (array<T>, function<T, U>)                       : array<U>
# MAGIC filter      (array<T>, function<T, Boolean>)                 : array<T>
# MAGIC exists      (array<T>, function<T, V, Boolean>)              : Boolean
# MAGIC reduce      (array<T>, B, function<B, T, B>, function<B, R>) : Union
# MAGIC ```

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([StructField("celsius", ArrayType(IntegerType()))])

t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
t_c = spark.createDataFrame(t_list, schema)
t_c.createOrReplaceTempView("tC_python")

display(t_c)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val t1 = Array(35, 36, 32, 30, 40, 42, 38)
# MAGIC val t2 = Array(31, 32, 34, 55, 56)
# MAGIC val tC = Seq(t1, t2).toDF("celsius")
# MAGIC tc.createOrReplaceTempView("tC_scala")
# MAGIC 
# MAGIC display(tc)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Transform()
# MAGIC `transform(array<T>, function<T, U>): array<U>`
# MAGIC 
# MAGIC The transform() function produces an array by applying a function to each element of the input array (similar to a map() function):

# COMMAND ----------

df = spark.sql("""
SELECT
  celsius,
  transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit
FROM tC_python
""")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Filter()
# MAGIC `filter(array<T>, function<T, Boolean>): array<T>`
# MAGIC 
# MAGIC The filter() function produces an array consisting of only the elements of the input array for which the Boolean function is true:

# COMMAND ----------

df = spark.sql("""
SELECT celsius,
  filter(celsius, t -> t > 38) as high
FROM tC_python
""")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exists()
# MAGIC `exists(array<T>, function<T, V, Boolean>): Boolean`
# MAGIC 
# MAGIC The exists() function returns true if the Boolean function holds for any element in the input array:

# COMMAND ----------

df = spark.sql("""
SELECT celsius, 
       exists(celsius, t -> t = 38) as threshold
  FROM tC
""")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Reduce()
# MAGIC `reduce(array<T>, B, function<B, T, B>, function<B, R>)`
# MAGIC 
# MAGIC The reduce() function reduces the elements of the array to a single value by merging the elements into a buffer B using function<B, T, B> and applying a finishing function<B, R> on the final buffer:

# COMMAND ----------

# calculate the average temperature and convert to F
df = spark.sql("""
SELECT
  celsius, 
  reduce(
    celsius,
    0,
    (t, acc) -> t + acc,
    acc -> (acc div size(celsius) * 9 div 5) + 32
  ) as avgFahrenheit
FROM tC
""")

display(df)
