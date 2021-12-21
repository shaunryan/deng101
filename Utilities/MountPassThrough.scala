// Databricks notebook source
// MAGIC %run AzureDataFactory/Includes/SetupEnvironment

// COMMAND ----------

dbutils.fs.unmount("/mnt/datascience")

// COMMAND ----------

val clientId = dbutils.secrets.get(scope = "key-vault-secrets", key = s"GC-DATALAKE-SPN-DATAFACTORY-${getEnvironment}-APPID")
val credential = dbutils.secrets.get(scope = "key-vault-secrets", key = s"GC-DATALAKE-SPN-DATAFACTORY-${getEnvironment}")
val directoryid = "4d13380f-b2dc-401e-ade8-03a67801c676"
val refreshUrl = s"https://login.microsoftonline.com/${directoryid}/oauth2/token"

val configs = Map(
  "fs.adl.oauth2.access.token.provider.type" -> "CustomAccessTokenProvider",
  "fs.adl.oauth2.access.token.custom.provider" -> spark.conf.get("spark.databricks.passthrough.adls.tokenProviderClassName"),
  "fs.adl.oauth2.client.id" -> clientId,
  "fs.adl.oauth2.credential" -> credential,
  "fs.adl.oauth2.refresh.url" -> refreshUrl)


dbutils.fs.mount(
  source = s"${getDataLakeStorageAccount}datascience",
  mountPoint = "/mnt/datascience",
  extraConfigs = configs)

// COMMAND ----------

display(dbutils.fs.ls("/mnt/datascience/"))
