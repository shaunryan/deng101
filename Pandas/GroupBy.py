# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Group By
# MAGIC 
# MAGIC 
# MAGIC  - Splits a DataFrame into group based on some criteria
# MAGIC  - Apply a function to each group independently
# MAGIC  - Combine the results into a DataFrame
# MAGIC  
# MAGIC Creating the group by object ionly varifies that you've created a valid mapping. It's not a dataframe but a group of dataframes in a dictionary structure.
# MAGIC  
# MAGIC  
# MAGIC  

# COMMAND ----------

# MAGIC %run "./Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### GroupBy Object

# COMMAND ----------

oo.groupby("Edition")

# COMMAND ----------

list(poo.groupby("Edition"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Iterate through Groups

# COMMAND ----------

for group_key, group_value in poo.groupby("Edition"):
  print(group_key)
  print(group_value)

# COMMAND ----------

type(group_value)

# COMMAND ----------

type(group_key)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### GroupBy Computations
# MAGIC 
# MAGIC - GroupBy.size()
# MAGIC - GroupBy.count()
# MAGIC - GroupBy.first(), GroupBy.last()
# MAGIC - GroupBy.head(), GroupBy.tail()
# MAGIC - GroupBy.mean()
# MAGIC - GroupBy.max(), GroupBy.min()
# MAGIC - GroupBy.agg() - passed using dictionary, list or a custom fuction

# COMMAND ----------

oo.groupby("Edition").size().head()

# COMMAND ----------

oo.groupby(["Edition", "NOC", "Medal"]).agg(["min","max","count"]).head()

# COMMAND ----------

oo.groupby(["Edition", "NOC", "Medal"]).agg("count").head()

# COMMAND ----------

poo.groupby(["Edition", "NOC", "Medal"]).agg({"Edition" : ["min","max","count"]}).head()

# COMMAND ----------

oo[oo.Athlete == "LEWIS, Carl"].groupby("Athlete").agg({"Edition" : ["min","max","count"]})

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Questions
# MAGIC 
# MAGIC 1. Using groupby, plot the total number of medals awarded at each of the olympic games throughout history
# MAGIC 2. Create a list showing the total number of medals won for each country over the hsitory of the olympics. For each country include the year of the first and most recent Olympic medal wins

# COMMAND ----------

# 1. Using groupby, plot the total number of medals awarded at each of the olympic games throughout history

poo.groupby("Edition").agg({"Medal" : "count"}).plot(kind="bar")

# COMMAND ----------

# or, note this sorts automatically
poo.groupby("Edition").size().plot(kind="bar")

# COMMAND ----------

# 1. Using groupby, plot the total number of medals awarded at each of the olympic games throughout history
poo.groupby("NOC").agg({"Edition" : ["min","max","count"]})
