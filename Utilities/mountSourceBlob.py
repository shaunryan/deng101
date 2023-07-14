# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------


secret = dbutils.secrets.get(
  scope = "azure-key-vault-scope", 
  key = "DATALAKE-BLB-KEY"
)

dbutils.fs.mount(
  source = "wasbs://landing@blbdatalakegeneva.blob.core.windows.net",
  mount_point = "/mnt/landing",
  extra_configs = {
    "fs.azure.account.key.blbdatalakegeneva.blob.core.windows.net":secret
})

# COMMAND ----------

dbutils.fs.ls("/mnt/landing/data/header_footer")

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

