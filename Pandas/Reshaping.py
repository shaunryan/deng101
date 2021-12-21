# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Reshaping
# MAGIC 
# MAGIC 
# MAGIC  - Stacking
# MAGIC  - Unstacking
# MAGIC 
# MAGIC Or pivoting and unpivoting
# MAGIC  
# MAGIC  

# COMMAND ----------

# MAGIC %run "./Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Stacking
# MAGIC 
# MAGIC Pulls the rows out into the columns

# COMMAND ----------

mw = poo[(poo.Edition==2008) & ((poo.Event == "100m") | (poo.Event == "200m"))]
g = mw.groupby(["NOC", "Gender", "Discipline","Event"]).size()
g

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Unstack
# MAGIC 
# MAGIC  - returns a series or dataframe
# MAGIC  - can fill na values using a parameter
# MAGIC  - level involved will automatically be sorted
# MAGIC  - moves rows to columns (makes it wider)

# COMMAND ----------

df = g.unstack(["Discipline","Event"])
df

# COMMAND ----------

# MAGIC %md
# MAGIC ###Stacking
# MAGIC 
# MAGIC  - returns dataframe or series
# MAGIC  - by default drops na
# MAGIC  - moves columns to rows (makes it taller)

# COMMAND ----------

df.stack(["Discipline","Event"])

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Questions
# MAGIC 
# MAGIC 1. Plot the number of gold medals won by the US male and female athletes throughout the history of the olympics?
# MAGIC 2. Plot the five atheletes who have won the most gold medals over the history of the Olympics. When there is a tie, consider the number of silver medals, and then bronze medals

# COMMAND ----------

# 1. Plot the number of gold medals won by the US male and female athletes throughout the history of the olympics?

poo[(poo.NOC=="USA") & (poo.Medal=="Gold")].groupby(["Gender", "Edition"]).size().unstack("Gender", fill_value=0).plot(kind="bar")


# COMMAND ----------

# 2. Plot the five atheletes who have won the most gold medals over the history of the Olympics. When there is a tie, consider the number of silver medals, and then bronze medals

g = poo.groupby(["Athlete", "Medal"]).size().unstack("Medal", fill_value=0)
g.sort_values(["Gold", "Silver", "Bronze"], ascending=False)[["Gold","Silver","Bronze"]].head(5).plot(kind="bar")

