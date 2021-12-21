# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # The Basics

# COMMAND ----------

# MAGIC %run "./Setup"

# COMMAND ----------

pd.show_versions()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Input & Validation

# COMMAND ----------

# pandas skiprows doesn't work.
oo = ks.read_csv(olympics, skiprows=None, delimiter=',')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Shape, Head, Tail, Info
# MAGIC 
# MAGIC Returns a tuple (rows and columns) that shows the dimensionality of the dataset

# COMMAND ----------

numberOfRows = oo.shape[0]
numberOfColumns = oo.shape[1]

print(f"""
Rows: {numberOfRows}
Cols: {numberOfColumns}
""")

oo.shape

# COMMAND ----------

oo.head()

# COMMAND ----------

oo.tail()

# COMMAND ----------

oo.info()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Series
# MAGIC 
# MAGIC ### Columns

# COMMAND ----------

# Accessing column series - 1 dimensional array of indexed data
# city = oo['City']
# city = oo["City"]
city = oo.City #can be a short cut but does not work with spaces in the name

cityEditionList = ["City","Edition"]
cityEdition = oo[cityEditionList]

city
#cityEdition

# COMMAND ----------

ts = type(city)
td = type(cityEdition)
print(f"""
city is a {ts}
cityEdition is a {td}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Basic Analysis
# MAGIC 
# MAGIC ### Value Counts
# MAGIC 
# MAGIC Count of unique values, order is the most frequent order.
# MAGIC 
# MAGIC Drop NA is on default

# COMMAND ----------

oo.Edition.value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC Split by Gender

# COMMAND ----------

oo.Gender.value_counts(ascending=True, dropna=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort Values
# MAGIC 
# MAGIC Can be a list or dataframe
# MAGIC the inplace option is false so sorting needs to be returned to a new variable

# COMMAND ----------

ath = oo.Athlete.sort_values()
ath

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC with the dataframe

# COMMAND ----------

oo_sorted = oo.sort_values(by=["Edition","Athlete"])
oo_sorted.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Boolean Indexing

# COMMAND ----------

oo.Medal == "Gold"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Filter using boolean indexing

# COMMAND ----------

oo[oo.Medal == "Gold"].head()

# COMMAND ----------

oo[(oo.Medal == "Gold") & (oo.Gender == "Women")].head()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### String Handling
# MAGIC 
# MAGIC Available to every series using the str attribute
# MAGIC 
# MAGIC <br/>
# MAGIC - Series.str.contains()
# MAGIC - Series.str.startsWith()
# MAGIC - Series.str.isNumeric()
# MAGIC ...

# COMMAND ----------

oo[oo.Athlete.str.contains("Florence")].head()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Basic Analysis Challenge
# MAGIC 
# MAGIC 1. In which events did Jesse Owens win a medal?
# MAGIC 2. Which country has won the most men's gold medals in singles badminton over the years? Sort the results alphabetically by the players name
# MAGIC 3. Which 3 countries have won the medals in recent years (from 1984 to 2008)
# MAGIC 4. Display the male gold medal winners for the 100m track abd field sprint event over the years

# COMMAND ----------

# 1 - In which events did Jesse Owens win a medal?
oo[(oo.Athlete == "OWENS, Jesse") & (oo.Medal == "Gold")][["Athlete", "Event", "Medal"]]

# COMMAND ----------

# 2 - Which country has won the most men's gold medals in singles badminton over the years? Sort the results alphabetically by the players name
oo[(oo.Medal=="Gold") & (oo.Sport=="Badminton") & (oo.Gender=="Men") & (oo.Event=="doubles")].sort_values(by=["NOC","Athlete"])


# COMMAND ----------

# 3 - Which 3 countries have won the medals in recent years (from 1984 to 2008)

oo[(oo.Edition>=1984) & (oo.Edition<= 2008) & (oo.Medal=="Gold")].NOC.value_counts().head(3)

# COMMAND ----------

#  4 - Display the male gold medal winners for the 100m track abd field sprint event over the years. Show most recent Olympic City, Edition & Country

oo[(oo.Medal=="Gold") & (oo.Discipline=="Athletics") & (oo.Event=="100m")][["City","Edition","Athlete","NOC"]].sort_values(by=["Edition"], ascending=False)
