# Databricks notebook source
import discover_modules
discover_modules.go(spark)

from utilities import AppConfig

app_config = AppConfig(dbutils, spark)
app_config.help()
app_config.connect_storage()

# COMMAND ----------

from pyspark.sql.types import *


database = "autoloader"
table = "autoloader"
checkpointPath = f"{app_config.get_storage_account()}checkpoint/raw.{database}.{table}/"

inputPath = f"{app_config.get_storage_account()}raw/autoloader/"
outputPath = f"{app_config.get_storage_account()}databricks/delta/{database}/{table}/"
schema = StructType([StructField('name', StringType(), False)])

# https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/auto-loader-gen2
configuration = {
  "database": database,
  "table": table,
  "checkpointPath": checkpointPath,
  "inputPath": inputPath,
  "outputPath": outputPath,
  "options" : {
    # auto loader options
    "cloudFiles.allowOverwrites"         : "true",
    "cloudFiles.format"                  : "json",
    "cloudFiles.includeExistingFiles"    : "true",
#     "cloudFiles.inferColumnTypes"        : "false",
#     "cloudFiles.maxBytesPerTrigger"      : None, # ignored for trigger once
#     "cloudFiles.maxFileAge"              : None, # not recommended unless loading millions of files per hour
#     "cloudFiles.resourceTag.mycustom"    : "mycustomtagvalue", # https://docs.microsoft.com/en-us/rest/api/storageservices/naming-queues-and-metadata
#     "cloudFiles.schemaEvolutionMode"     : None, # "addNewColumns" when a schema is not provided
#     "cloudFiles.schemaHints"             : None, # Schema info that you provide to Auto Loader during schema inference. https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/auto-loader-schema
#     "cloudFiles.schemaLocation"          : None, # The location to store the inferred schema and subsequent changes. See schema inference for more details.
#     "cloudFiles.backfillInterval"        : None, # "1 day" or "1 week" guarantee 100% delivery of all files.
#     "cloudFiles.useIncrementalListing"   : "auto", # "auto", "true", "false" full or incremental listing, autoloader tries to do the best it can
#     "cloudFiles.fetchParallelism"        : 1, # number of threads to use fetching from the queue
#     "cloudFiles.pathRewrites"            : None, # repoint abfss paths at dbfs mounts e.g. {"<container>@<storage-account>/path": "dbfs:/mnt/data-warehouse"}
#     "cloudFiles.queueName"               : app_config.get_queue_name(), #get_queue_name consumes events from queue instead of creating Azure Event Grid and Queue Storage services. cloudFiles.connectionString requires only read permissions on the queue
    
    "cloudFiles.queueName"               : "databricks-autoloader",
    "cloudFiles.useNotifications"        : "true", # use file notifications to determine new files instead of directory listing
    "cloudFiles.clientId"                : app_config.get_service_principal_id(),
    "cloudFiles.clientSecret"            : app_config.get_service_credential(),
    "cloudFiles.connectionString"        : app_config.get_queue_connection(), #The connection string for the queue, based on either account access key or shared access signature (SAS).
    "cloudFiles.maxFilesPerTrigger"      : "1000",
    "cloudFiles.validateOptions"         : "true",
    "cloudFiles.resourceGroup"           : app_config.get_resource_group(),
    "cloudFiles.subscriptionId"          : app_config.get_subscription_id(),
    "cloudFiles.tenantId"                : app_config.get_azure_ad_id()
  }
}

from pprint import pprint
pprint(configuration)

# COMMAND ----------

options = configuration["options"]
df = (spark.readStream.format("cloudFiles") 
      .options(**options) 
      .schema(schema) 
      .load(inputPath))

(
  df.writeStream.format("delta") 
  .option("checkpointLocation", checkpointPath) 
  .trigger(once=True) 
  .start(outputPath)
)


# COMMAND ----------


sqlCreateDb = f"create database if not exists {database}"

sqlCreateTable = f"""
  create table if not exists {database}.{table} (
    name string
  )
  using delta
  location "{outputPath}"
"""

sqlSelect = f"select * from {database}.{table}"

spark.sql(sqlCreateDb)
spark.sql(sqlCreateTable)
df = spark.sql(sqlSelect)

display(df)

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------


sqlCleanup = f"drop table if exists {database}.{table}"
spark.sql(sqlCleanup)

sqlCleanup = f"drop database if exists {database}"
spark.sql(sqlCleanup)

dbutils.fs.rm(f"{app_config.get_storage_account()}checkpoint/raw.{database}.{table}", True)
dbutils.fs.rm(f"{app_config.get_storage_account()}databricks/delta/{database}", True)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val scope = sys.env.get("AUTOMATIONSCOPE").get
# MAGIC val connectionString = dbutils.secrets.get(scope=scope, key="QUEUECONNECTION")
# MAGIC val clientSecret = dbutils.secrets.get(scope=scope, key="DATALAKE-SPN-CREDENTIAL") 
# MAGIC val clientId = dbutils.secrets.get(scope=scope, key="DATALAKE-SPN-APPID")
# MAGIC val storageAccount = sys.env.get("STORAGEACCOUNT").get
# MAGIC val path = s"${storageAccount}raw/autoloader/"
# MAGIC 
# MAGIC val resourceGroup = sys.env.get("RESOURCEGROUP").get
# MAGIC val subscriptionId = sys.env.get("SUBSCRIPTIONID").get
# MAGIC val tenantId = sys.env.get("AZUREADID").get

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // create event grid
# MAGIC 
# MAGIC import com.databricks.sql.CloudFilesAzureResourceManager
# MAGIC val manager = CloudFilesAzureResourceManager
# MAGIC   .newManager
# MAGIC   .option("cloudFiles.connectionString", connectionString)
# MAGIC   .option("cloudFiles.resourceGroup", resourceGroup)
# MAGIC   .option("cloudFiles.subscriptionId", subscriptionId)
# MAGIC   .option("cloudFiles.tenantId", tenantId)
# MAGIC   .option("cloudFiles.clientId", clientId)
# MAGIC   .option("cloudFiles.clientSecret", clientSecret)
# MAGIC   .option("path", path) // required only for setUpNotificationServices
# MAGIC   .create()
# MAGIC 
# MAGIC 
# MAGIC // Set up an AQS queue and an event grid subscription associated with the path used in the manager. Available in Databricks Runtime 7.4 and above.
# MAGIC manager.setUpNotificationServices("autoloader")
# MAGIC 
# MAGIC // List notification services created by Auto Loader
# MAGIC // val svcs = manager.listNotificationServices()
# MAGIC // display(svcs)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // tear down
# MAGIC 
# MAGIC import com.databricks.sql.CloudFilesAzureResourceManager
# MAGIC val manager = CloudFilesAzureResourceManager
# MAGIC   .newManager
# MAGIC   .option("cloudFiles.connectionString", connectionString)
# MAGIC   .option("cloudFiles.resourceGroup", resourceGroup)
# MAGIC   .option("cloudFiles.subscriptionId", subscriptionId)
# MAGIC   .option("cloudFiles.tenantId", tenantId)
# MAGIC   .option("cloudFiles.clientId", clientId)
# MAGIC   .option("cloudFiles.clientSecret", clientSecret)
# MAGIC   .create()
# MAGIC 
# MAGIC 
# MAGIC // List notification services created by Auto Loader
# MAGIC val svcs = manager.listNotificationServices()
# MAGIC 
# MAGIC // Tear down the notification services created for a specific stream ID.
# MAGIC svcs.collect().foreach( svc => 
# MAGIC   manager.tearDownNotificationServices(svc.getAs("streamId"))
# MAGIC )

# COMMAND ----------

# MAGIC %scala
# MAGIC val svcsClean = manager.listNotificationServices()
# MAGIC display(svcsClean)
