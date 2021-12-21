// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/azure-datalake-gen2
// MAGIC 
// MAGIC 
// MAGIC  - Mounting an Azure Data Lake Storage Gen2 is supported only using OAuth credentials. Mounting with an account access key is not supported.
// MAGIC  - All users in the Azure Databricks workspace have access to the mounted Azure Data Lake Storage Gen2 account. The service client that you use to access the Azure Data Lake Storage Gen2 account should be granted access only to that Azure Data Lake Storage Gen2 account; it should not be granted access to other resources in Azure.
// MAGIC  - Once a mount point is created through a cluster, users of that cluster can immediately access the mount point. To use the mount point in another running cluster, you must run dbutils.fs.refreshMounts() on that running cluster to make the newly created mount point available for use.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Basic Passthrough

// COMMAND ----------

import fathom_core.Configuration

// COMMAND ----------

storageAccount = Configuration.getStorageAccount()
dbutils.fs.ls(storageAccount)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Passthrough Configs

// COMMAND ----------


def getPrincipalConfig(servicePrincipalId:String,
          serviceCredential:String,
          azureADRefreshUrl:String) = 
{
      
  val baseProperty = "fs.azure.account"
  
  Map(s"${baseProperty}.auth.type"            -> "OAuth",
      s"${baseProperty}.oauth.provider.type"     -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
      s"${baseProperty}.oauth2.client.id"        -> servicePrincipalId,
      s"${baseProperty}.oauth2.client.secret"    -> serviceCredential,
      s"${baseProperty}.oauth2.client.endpoint"  -> azureADRefreshUrl)
}


// COMMAND ----------

def getADConfig() = 
{
      
  val baseProperty = "fs.azure.account"
  val provider = spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
  
  Map(s"${baseProperty}.auth.type": "CustomAccessToken",
      s"${baseProperty}.custom.token.provider.class": provider)
}



// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Mounted AD Passthrough

// COMMAND ----------

val mountname = "datalake"
dbutils.fs.mount(
  source = storageAccount,
  mount_point = s"/mnt/${mountname}",
  extra_configs = getADConfig())

dbutils.fs.ls(s"/mnt/${mountname}")

// COMMAND ----------

dbutils.fs.unmount(s"/mnt/${mountname}")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Mounted Service Principal

// COMMAND ----------

import fathom_core.Configuration

val config = getPrincipalConfig(
          Configuration.getServicePrincipalId(),
          Configuration.getServiceCredential(),
          Configuration.getOAuthRefreshUrl()
)

// COMMAND ----------

val mountname = "datalake"
dbutils.fs.mount(
  source = Configuration.getStorageAccount(),
  mount_point = s"/mnt/${mountname}",
  extra_configs = config)

dbutils.fs.ls(s"/mnt/${mountname}")
