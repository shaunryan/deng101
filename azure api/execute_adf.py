# Databricks notebook source
subscription_id = "e95203c2-64a0-43f9-bfc5-a4fdc588571a"
resource_group = "DataPlatform"
pipeline = "test"
adf = "DataPlatfromRhone-ADF"

url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{adf}/pipelines/{pipeline}/createRun?api-version=2018-06-01"
url

# COMMAND ----------


