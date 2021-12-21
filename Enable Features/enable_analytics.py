# Databricks notebook source
# MAGIC %sh
# MAGIC curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# COMMAND ----------

# MAGIC %sh
# MAGIC az login

# COMMAND ----------

# MAGIC %sh
# MAGIC #!/bin/bash
# MAGIC RESOURCE_GROUP=DataPlatform
# MAGIC DATABRICKS_ID=$(az resource list --resource-group $RESOURCE_GROUP --resource-type Microsoft.Databricks/workspaces --query [0].id -otsv)
# MAGIC DATABRICKS_URL="https://"$(az resource list --resource-group $RESOURCE_GROUP --resource-type Microsoft.Databricks/workspaces --query [0].location -otsv)".azuredatabricks.net"
# MAGIC TENANT_ID=$(az account show --query tenantId -otsv)
# MAGIC 
# MAGIC GLOBAL_DATABRICKS_APPID=2ff814a6-3304-4ab8-85cb-cd0e6f879c1d
# MAGIC AZURE_MANAGEMENT=https://management.azure.com
# MAGIC aztoken=$(az account get-access-token --resource $AZURE_MANAGEMENT --query accessToken -otsv)
# MAGIC 
# MAGIC az rest --method PATCH --resource $GLOBAL_DATABRICKS_APPID --uri $DATABRICKS_URL/api/2.0/workspace-conf --headers X-Databricks-Azure-SP-Management-Token="$aztoken" X-Databricks-Azure-Workspace-Resource-Id="$DATABRICKS_ID" --body '{"enableProjectTypeInWorkspace": "true"}'
