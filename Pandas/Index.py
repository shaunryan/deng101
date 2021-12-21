# Databricks notebook source
# MAGIC %md
# MAGIC # Indexing
# MAGIC 
# MAGIC - The index object is an immutable array
# MAGIC - Indexing allows you to access a row or column using a label

# COMMAND ----------

# MAGIC %run "./Setup"

# COMMAND ----------

# pandas skiprows doesn't work.
oo = ks.read_csv(olympics, skiprows=None, delimiter=',')

# COMMAND ----------

noc = ks.read_csv(countryCodes, skiprows=None, delimiter=',')

# COMMAND ----------

noc.head()

# COMMAND ----------

oo.head()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Index

# COMMAND ----------

# has it's own type
type(oo.index)

# COMMAND ----------

# appears to be a koalas incompatability
oo.index[100]

# COMMAND ----------

# so switching to pandas
poo = oo.to_pandas()
poo.index[100]

# COMMAND ----------

# it's immutable so we can't write to it

poo.index[100] = 5

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### set_index()
# MAGIC 
# MAGIC  - Set the dataframe using 1 or more columns
# MAGIC  - set_index(keys, inplace=True)
# MAGIC  - advantage of pandas we can use labels as the index

# COMMAND ----------

poo.head()

# COMMAND ----------

poo.set_index("Athlete")

# COMMAND ----------

poo.head() 
# note that it's not retained or done in place

# COMMAND ----------

ath = poo.set_index("Athlete")
# or
poo.set_index("Athlete", inplace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### reset_index()
# MAGIC 
# MAGIC - reverts back

# COMMAND ----------

poo.reset_index(inplace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### sort_index
# MAGIC 
# MAGIC - sorting reduces the time requried to access any subset of that data using the index

# COMMAND ----------

ath.head()

# COMMAND ----------

ath.sort_index(inplace=True)
ath.head()

# COMMAND ----------

ath.sort_index(inplace=True, ascending=False)
ath.head()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### loc[]
# MAGIC 
# MAGIC - on DataFrame and Series
# MAGIC - a label based indexer for selection by label
# MAGIC - loc[] will raise a KeyError when the items are not found

# COMMAND ----------

oo.loc['Bolt, Usain']
# errors because we need to set the index

# COMMAND ----------

ath = oo.set_index("Athlete")
ath.loc['BOLT, Usain']

# COMMAND ----------

oo[oo.Athlete == "BOLT, Usain"]

# COMMAND ----------

# can also use loc the same
oo.loc[oo.Athlete == "BOLT, Usain"]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### ilocp[]
# MAGIC 
# MAGIC - iloc[] is primarily integer position based (from 0 to length-1 of the axis)
# MAGIC - Allows traditional Pythonic slicing
# MAGIC - use iloc if already indexed and sorted as you need

# COMMAND ----------

  oo.iloc[1700]

# COMMAND ----------

oo.iloc[[1542, 2390, 6000, 15000]]

# COMMAND ----------

oo.iloc[1:4]

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Questions
# MAGIC 
# MAGIC  1. plot the total number of medals awarded at each of the Olympic games through out history
# MAGIC  2. which countries did not win a medal in the 2008 Olympics? How many countries were there?

# COMMAND ----------

# 1. plot the total number of medals awarded at each of the Olympic games through out history

# note that Edition returns a series, the year is the index 
# so needs to be sorted using the sort_index() function
oo.Edition.value_counts().sort_index().plot(kind="bar")

# COMMAND ----------

# 2. which countries did not win a medal in the 2008 Olympics? How many countries were there?

lo = oo[oo.Edition==2008]




# COMMAND ----------

noc.set_index('Int Olympic Committee code', inplace=True)
noc.head()

# COMMAND ----------

medal_2008 = lo.NOC.value_counts()
medal_2008

# COMMAND ----------

# when we add a series it
# automatically joins on the index which is the NOC in both data frames
noc['medal_2008'] = medal_2008

# COMMAND ----------

# Pandas version

poo = oo.to_pandas()
pnoc = noc.to_pandas()

# COMMAND ----------

plo = poo[poo.Edition==2008]

# COMMAND ----------

pnoc.set_index('Int Olympic Committee code', inplace=True)
pnoc.head()

# COMMAND ----------

pmedal_2008 = plo.NOC.value_counts()


# COMMAND ----------

pnoc['medal_2008'] = pmedal_2008

pnoc[pnoc.medal_2008.isnull()]
