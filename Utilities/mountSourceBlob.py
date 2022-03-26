# Databricks notebook source
import discover_modules
discover_modules.go(spark)

# COMMAND ----------

from utilities import AppConfig

app_config = AppConfig(dbutils, spark)
app_config.help()

# COMMAND ----------

mountname = "datalake"
try:
  dbutils.fs.unmount(f"/mnt/{mountname}")
except:
  print("datalake isn't mounted")


# COMMAND ----------


extra_configs = {"fs.azure.account.auth.type"               : "OAuth",
           "fs.azure.account.oauth.provider.type"     : "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id"        : app_config.get_service_principal_id(),
           "fs.azure.account.oauth2.client.secret"    : app_config.get_service_credential(),
           "fs.azure.account.oauth2.client.endpoint"  : app_config.get_oauth_refresh_url()}


dbutils.fs.mount(
  source = app_config.get_storage_account(),
  mount_point = f"/mnt/{mountname}",
  extra_configs = extra_configs)


# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{mountname}"))
