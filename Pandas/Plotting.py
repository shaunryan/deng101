# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Plotting

# COMMAND ----------

# MAGIC %run "./Setup"

# COMMAND ----------

# MAGIC %config InlineBackend.figure_format = 'retina'
# MAGIC 
# MAGIC from IPython.display import set_matplotlib_formats
# MAGIC set_matplotlib_formats('retina')

# COMMAND ----------

# MAGIC %config InlineBackend.figure_format = 'png2x'
# MAGIC 
# MAGIC from IPython.display import set_matplotlib_formats
# MAGIC set_matplotlib_formats('png2x')

# COMMAND ----------

# set_matplotlib_formats('png')

# %config InlineBackend.figure_format = 'png'

# COMMAND ----------

import matplotlib.pyplot as plt


# COMMAND ----------

# MAGIC %matplotlib inline

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Plot Types
# MAGIC 
# MAGIC ### What were the different sports in the first olympics? Plot them using different graphs.

# COMMAND ----------

fo = oo[oo.Edition == 1896]
fo.head()

# COMMAND ----------

fo.Sport.value_counts().plot(kind='line')

# COMMAND ----------

fo.Sport.value_counts().plot(kind='bar')

# COMMAND ----------

fo.Sport.value_counts().plot(kind='barh')

# COMMAND ----------

fo.Sport.value_counts().plot(kind='pie')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Plot Colours
# MAGIC https://matplotlib.org/3.3.3/gallery/color/named_colors.html

# COMMAND ----------

fo.Sport.value_counts().plot(color="plum")

# COMMAND ----------

fo.Sport.value_counts().plot(kind="bar", color="maroon")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Figure Size

# COMMAND ----------

fo.Sport.value_counts().plot(figsize=(10,3)) # is in inches

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Colormaps
# MAGIC 
# MAGIC - Sequential - for information that has ordering
# MAGIC - Diverging - diviates around a middle value
# MAGIC - Qualitative - misc colors, no ordering or relation

# COMMAND ----------

fo.Sport.value_counts().plot(kind='pie', colormap="Pastel1")

# COMMAND ----------

# MAGIC %md
# MAGIC # Seaborn - Basic Plotting
# MAGIC 
# MAGIC Creates nice looking plots, works well with pandas and matlibplot
# MAGIC https://seaborn.pydata.org/examples/index.html

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # seaborn.countrplot
# MAGIC 
# MAGIC Dealing with more statistical data, categorical data or requiring more advanced plots

# COMMAND ----------

import seaborn as sns

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### How many medals have been won by men and women in the history of the Olympics? 
# MAGIC ### How many gold, silver and bronze medals were won for each gender?

# COMMAND ----------

sns.countplot(x="Medal", data=oo, hue="Gender")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Challenge
# MAGIC 
# MAGIC 1. Plot the number of medals achieved by the Chinese team (men and women) in Beijing 2008 using Matplotlib, Seaborn. How can you use color maps to give the data more meaning?
# MAGIC 2. Plot the number of gold, silver and bronze medals for each gender. How can you give the data more meaning? Is there anything else you can change to make it more intuitive?

# COMMAND ----------


oo[(oo.Edition==2008) & (oo.City=="Beijing") & (oo.NOC=="CHN")].Medal.value_counts().plot(kind="bar")

# COMMAND ----------

data = oo[(oo.Edition==2008) & (oo.City=="Beijing") & (oo.NOC=="CHN")][["Medal", "Gender"]]

sns.countplot(x="Gender", data=oo, palette="bwr")

# COMMAND ----------

data = oo[(oo.Edition==2008) & (oo.City=="Beijing") & (oo.NOC=="CHN")][["Medal", "Gender"]]

sns.countplot(x="Medal", data=oo, hue="gender", palette="bwr", order=["Gold", "Silver", "Bronze"])
