# Databricks notebook source
# MAGIC %sh
# MAGIC 
# MAGIC cat /dbfs/FileStore/cluster/init/config_small_8.0.sh

# COMMAND ----------

def connect_storage():

    _spark = spark


    _gen1 = "dfs.adls.oauth2"
    _gen2 = "fs.azure.account"


    _spark.conf.set(f"{_gen2}.auth.type", "OAuth")
    _spark.conf.set(f"{_gen2}.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    _spark.conf.set(f"{_gen2}.oauth2.client.id", get_service_principal_id())
    _spark.conf.set(f"{_gen2}.oauth2.client.secret", get_service_credential())
    _spark.conf.set(f"{_gen2}.oauth2.client.endpoint", get_oauth_refresh_url())

    

    print(f"""
      |Connected:
      |-----------------------------------------------
      | environment = {get_environment()}
      | storage account = {get_storage_account()} 
    """)

# COMMAND ----------

from pprint import pprint


# COMMAND ----------

# # not compatible or needed on a passthrough AD cluster!
# from fathom.ConnectStorage import connect_storage

from fathom.Configuration import *
connect_storage()


# COMMAND ----------

from fathom.Configuration import help

displayHTML(help(True))

# COMMAND ----------

from fathom import Configuration as config

config.help()

# COMMAND ----------

display(dbutils.fs.ls(f"{get_storage_account()}/raw/contoso/retaildw/data/2007/01/01"))

# COMMAND ----------

dbutils.fs.unmount("/mnt/test")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/test"))

# COMMAND ----------

dbutils.fs.ls("abfss://datalake@datalakegeneva.dfs.core.windows.net")
