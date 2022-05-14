# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### 1. Setup Expectations

# COMMAND ----------

# MAGIC %pip install great-expectations==0.14.12

# COMMAND ----------

# dbutils.widgets.removeAll()
# dbutils.widgets.text("ProcessGroup", "adworks", "ProcessGroup")
# dbutils.widgets.text("Area", "adworks", "Area")
# dbutils.widgets.text("Entity", "sales", "Entity")
# dbutils.widgets.text("Stage", "base", "Stage")
# dbutils.widgets.text("SliceDate", "2022-03-27", "SliceDate")
# dbutils.widgets.text("ADFExecutionID", "-1", "ADFExecutionID")
# dbutils.widgets.text("ProcessID", "-1", "ProcessID")

# COMMAND ----------


process_group = dbutils.widgets.get("ProcessGroup").lower()
entity = dbutils.widgets.get("Entity").lower()
area =  dbutils.widgets.get("Area").lower()
stage = dbutils.widgets.get("Stage").lower()
slice_date = dbutils.widgets.get("SliceDate").lower()
adf_execution_id = dbutils.widgets.get("ADFExecutionID").lower()
process_id = dbutils.widgets.get("ProcessID").lower()

format = "csv"
name = f"{area}_{process_group}_{stage}_{entity}"
root = "/FileStore/temp/adworks/upload/sales"
file = f"{name}.{format}"
dbx_host = 'adb-8723178682651460.0.azuredatabricks.net'

expectation_suite_name = f"{name}_suite"

print(f"""
process_group : {process_group}
process_id: {process_id}
entity : {entity}
area : {area}
stage : {stage}
format : {format}
name : {name}
root : {root}
file : {file}
dbx_host : {dbx_host}
expectation_suite_name : {expectation_suite_name}
""")



# COMMAND ----------

import datetime

import pandas as pd
from ruamel import yaml

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)

# COMMAND ----------

expectations_path = f"{root}/expectations"
root_directory = f"/dbfs{expectations_path}"

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=root_directory
    ),
)
context = BaseDataContext(project_config=data_context_config)



# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Prepare Data

# COMMAND ----------

############################# CUSTOMISE ############################
# mostly we'll be testing against base so shouldn't need any customisation
# but maybe customised as you see fit

from pyspark.sql.types import *


query = f"select * from `{stage}_{area}`.`{entity}`"
df = (spark.sql(query))

df.createOrReplaceTempView(name)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 3. Connect to Data

# COMMAND ----------

datasource_config = {
    "name": name,
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        name: {
            "module_name": "great_expectations.datasource.data_connector",
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": [
                "run_id",
                "slice_date",
                "adf_execution_id",
                "process_id"
            ],
        }
    },
}
context.test_yaml_config(yaml.dump(datasource_config))

# COMMAND ----------

context.add_datasource(**datasource_config)

# COMMAND ----------

batch_request = RuntimeBatchRequest(
    datasource_name = name,
    data_connector_name = name,
    data_asset_name = name,  # This can be anything that identifies this data_asset for you
    batch_identifiers = {
        "run_id": f"{datetime.date.today().strftime('%Y%m%d')}",
        "slice_date": slice_date,
        "adf_execution_id" : adf_execution_id,
        "process_id": process_id
    },
    runtime_parameters={"batch_data": df},  # Your dataframe goes here
)
batch_request

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 5. Validate Data

# COMMAND ----------

checkpoint_name = f"checkpoint_{name}_base"
checkpoint_config = {
    "name": checkpoint_name,
    "config_version": 1.0,
    "class_name": "SimpleCheckpoint",
    "run_name_template": "base-profile-%Y%m%d-%H%M%S",
}

# COMMAND ----------

checkpoint = context.test_yaml_config(yaml.dump(checkpoint_config))
context.add_checkpoint(**checkpoint_config)

# COMMAND ----------


checkpoint_result = context.run_checkpoint(
    checkpoint_name=checkpoint_name,
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 6. Export Data Report

# COMMAND ----------

workDir = "file:/tmp/expectations"
docs_path = f"{root}/expectations/uncommitted/data_docs/local_site"
tar_postfix = "ge.tar.gz"
from_tar_file = f"{area}_{process_group}_{tar_postfix}"

dbutils.fs.rm(from_tar_file, True)
dbutils.fs.rm(workDir, True)
dbutils.fs.mkdirs(workDir)

print(f"Copy expectation files from {docs_path} to {workDir}")
dbutils.fs.cp(docs_path, workDir, True)


# COMMAND ----------

# MAGIC %sh
# MAGIC tar -czvf /tmp/adwors_sales_ge.tar.gz /tmp/expectations

# COMMAND ----------

to_tar_file = f"{name}_{tar_postfix}"

tar_dir = f"file:/tmp/{from_tar_file}"
docs_path = f"{root}/{to_tar_file}"

print(f"Copy expectation files from {tar_dir} to {docs_path}")
dbutils.fs.cp(tar_dir, docs_path)
dbutils.fs.rm(tar_dir, True)
dbutils.fs.rm(workDir, True)

# COMMAND ----------

url = root.replace('FileStore','files')

displayHTML(f"""
<p>
<li>Download the tar ball of the exceptions report. </li>
<li>Unpack using the command `tar -xvf {to_tar_file}`. </li>
<li>Run the /tmp/expectations/index.html in a browser. </li>
</p>

<a href='https://{dbx_host}{url}/{to_tar_file}'>tar -xvf {to_tar_file}</a>

""")
