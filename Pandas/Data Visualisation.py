# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Data Visualisation
# MAGIC 
# MAGIC 
# MAGIC  
# MAGIC  

# COMMAND ----------

# MAGIC %run "./Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Heatmap
# MAGIC 
# MAGIC Present the summary of total medals won by participating countries in the 2008 Olympics

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt

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

# MAGIC %matplotlib inline

# COMMAND ----------

lo = poo[poo.Edition == 2008]
g = lo.groupby(["NOC","Medal"]).size().unstack("Medal", fill_value=0)
g = g.sort_values(["Gold","Silver","Bronze"], ascending=False)[["Gold","Silver","Bronze"]]
g

# COMMAND ----------

g = g.transpose()
g

# COMMAND ----------

plt.figure(figsize=(16,5))
sns.heatmap(g, cmap="Reds")

# COMMAND ----------

# 2. Plot the five atheletes who have won the most gold medals over the history of the Olympics. When there is a tie, consider the number of silver medals, and then bronze medals

g = poo.groupby(["Athlete", "Medal"]).size().unstack("Medal", fill_value=0)
g.sort_values(["Gold", "Silver", "Bronze"], ascending=False)[["Gold","Silver","Bronze"]].head(5).plot(kind="bar")


# COMMAND ----------

from matplotlib.colors import ListedColormap

# COMMAND ----------

sns.color_palette()

# COMMAND ----------

sns.palplot(sns.color_palette())

# COMMAND ----------

gsb = ["#dbb40c","#c5c9c7","#a87900"]
sns.palplot(gsb)

# COMMAND ----------

g = poo.groupby(["Athlete", "Medal"]).size().unstack("Medal", fill_value=0)
g = g.sort_values(["Gold", "Silver", "Bronze"], ascending=False)[["Gold","Silver","Bronze"]].head(5)
g.plot(kind="bar", colormap=gsb)

# COMMAND ----------

# MAGIC %md
# MAGIC # In every Olympics, which US athlete has won the most total number of medals? Include the Athelete's discipline

# COMMAND ----------

gy=poo[poo.NOC=="USA"]
gy = gy.groupby(["Edition","Athlete","Medal"]).size().unstack("Medal", fill_value=0)
gy["Total"] = gy["Gold"] +  gy["Silver"] + gy["Bronze"]
gy.reset_index(inplace=True)

tu = [group.sort_values("Total", ascending=False)[:1] for year, group in gy.groupby("Edition")]

top = pd.DataFrame()
# populate the data frame using the elements in the list
for i in tu:
  top = top.append(i)

top
