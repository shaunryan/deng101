# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/azure-datalake-gen2
# MAGIC 
# MAGIC 
# MAGIC  - Mounting an Azure Data Lake Storage Gen2 is supported only using OAuth credentials. Mounting with an account access key is not supported.
# MAGIC  - All users in the Azure Databricks workspace have access to the mounted Azure Data Lake Storage Gen2 account. The service client that you use to access the Azure Data Lake Storage Gen2 account should be granted access only to that Azure Data Lake Storage Gen2 account; it should not be granted access to other resources in Azure.
# MAGIC  - Once a mount point is created through a cluster, users of that cluster can immediately access the mount point. To use the mount point in another running cluster, you must run dbutils.fs.refreshMounts() on that running cluster to make the newly created mount point available for use.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Basic Passthrough

# COMMAND ----------

from fathom.Configuration import *

# COMMAND ----------

storageAccount = getStorageAccount()
dbutils.fs.ls(storageAccount)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passthrough Configs

# COMMAND ----------


def getPrincipalConfig(servicePrincipalId:str,
          serviceCredential:str,
          azureADRefreshUrl:str):
      
  baseProperty = "fs.azure.account"
  
  configs = {f"{baseProperty}.auth.type"               : "OAuth",
             f"{baseProperty}.oauth.provider.type"     : "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
             f"{baseProperty}.oauth2.client.id"        : servicePrincipalId,
             f"{baseProperty}.oauth2.client.secret"    : serviceCredential,
             f"{baseProperty}.oauth2.client.endpoint"  : azureADRefreshUrl}

  return configs



# COMMAND ----------


def getADConfig():
      
  baseProperty = "fs.azure.account"

  configs = { 
    f"{baseProperty}.auth.type": "CustomAccessToken",
    f"{baseProperty}.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
  }
   
  return configs




# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Mounted AD Passthrough

# COMMAND ----------

mountname = "datalake"
dbutils.fs.mount(
  source = storageAccount,
  mount_point = f"/mnt/{mountname}",
  extra_configs = getADConfig())

dbutils.fs.ls(f"/mnt/{mountname}")


# COMMAND ----------

dbutils.fs.unmount(f"/mnt/{mountname}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Mounted Service Principal

# COMMAND ----------

from fathom.Configuration import *
config = getPrincipalConfig(
          getServicePrincipalId(),
          getServiceCredential(),
          getOAuthRefreshUrl()
)


# COMMAND ----------

mountname = "datalake"
dbutils.fs.mount(
  source = getStorageAccount(),
  mount_point = f"/mnt/{mountname}",
  extra_configs = config)

dbutils.fs.ls(f"/mnt/{mountname}")


# COMMAND ----------

dbutils.fs.unmount(f"/mnt/{mountname}")
