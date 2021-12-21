# Databricks notebook source
# Connect storage, import libs & load data

# COMMAND ----------

from fathom.Configuration import *
from fathom.ConnectStorage import ConnectStorage

# ConnectStorage()
path = f"{getStorageAccount()}raw/pandas/"

countryCodes = dbutils.fs.ls(path)[0].path
olympics = dbutils.fs.ls(path)[1].path
display(dbutils.fs.ls(path))

# COMMAND ----------

import numpy as np
import pandas as pd
import databricks.koalas as ks
import pandas as pd

# pandas skiprows doesn't work.
print("Loading olympics into Koalas dataframe oo")
oo = ks.read_csv(olympics, skiprows=None, delimiter=',')
print("Loading countries into Koalas dataframe noc")
noc = ks.read_csv(countryCodes, skiprows=None, delimiter=',')
print("Loading olympics into Pandas dataframe poo")
poo = oo.to_pandas()
print("Loading countries into Pandas dataframe poo")
pnoc = noc.to_pandas()

# COMMAND ----------

oo.head()

# COMMAND ----------

noc.head()
