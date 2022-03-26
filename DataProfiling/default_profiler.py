# Databricks notebook source
# dbutils.widgets.removeAll()
# dbutils.widgets.text("process_group", "processgroup", "process group")
# dbutils.widgets.text("area", "area", "area")
# dbutils.widgets.text("entity", "entity", "entity")
# dbutils.widgets.text("stage", "stage", "stage")

# COMMAND ----------

# MAGIC %pip install great-expectations

# COMMAND ----------


process_group = dbutils.widgets.get("process_group")
area = dbutils.widgets.get("area")
entity = dbutils.widgets.get("entity")
stage = dbutils.widgets.get("stage")
datasource_name = f"{area}.{process_group}.{stage}.{entity}"
suite_name = f"{area}.{process_group}.{stage}.{entity}"
checkpoint_name = f"{area}.{process_group}.{stage}.{entity}.profile"

root_sub_path = f"expectations/{area}/{process_group}"
root_path = f"FileStore/{root_sub_path}"
ge_path = f"{root_path}/ge"
ge_root_directory = f"/dbfs/{ge_path}"
org_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
host = f"adb-{org_id}.0.azuredatabricks.net"

print(f"""
process_group : {process_group}
area : {area}
entity : {entity}
stage : {stage}
datasource_name : {datasource_name}
suite_name : {suite_name}
checkpoint_name : {checkpoint_name}
root_sub_path : {root_sub_path}
root_path : {root_path}
ge_path : {ge_path}
ge_root_directory : {ge_root_directory}
org_id : {org_id}
host : {host}
""")

# COMMAND ----------

dbutils.fs.rm(ge_path, True)
dbutils.fs.mkdirs(ge_path)

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

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=ge_root_directory
    ),
)
context = BaseDataContext(project_config=data_context_config)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Get Data

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz").limit(100).select("VendorID", "tip_amount", "store_and_fwd_flag")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Setup Data Source

# COMMAND ----------

datasource_config = {
    "name": datasource_name,
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        datasource_name: {
            "module_name": "great_expectations.datasource.data_connector",
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": [
                "run_id",
            ],
        }
    },
}

# COMMAND ----------

context.test_yaml_config(yaml.dump(datasource_config))

# COMMAND ----------

context.add_datasource(**datasource_config)

# COMMAND ----------

batch_request = RuntimeBatchRequest(
    datasource_name=datasource_name,
    data_connector_name=datasource_name,
    data_asset_name=datasource_name,  # This can be anything that identifies this data_asset for you
    batch_identifiers={
        "run_id": f"{datasource_name}_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={"batch_data": df},  # Your dataframe goes here
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Create Expectations

# COMMAND ----------

suite_name = suite_name
context.create_expectation_suite(
    expectation_suite_name=suite_name, overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name,
)

print(validator.head())

# COMMAND ----------

ignored_columns = []

# COMMAND ----------

from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler


profiler = UserConfigurableProfiler(
    profile_dataset=validator,
    excluded_expectations=None,
    ignored_columns=ignored_columns,
    not_null_only=False,
    primary_or_compound_key=False,
    semantic_types_dict=None,
    table_expectations_only=False,
    value_set_threshold="MANY",
)
suite = profiler.build_suite()

# COMMAND ----------

print(validator.get_expectation_suite(discard_failed_expectations=False))
validator.save_expectation_suite(discard_failed_expectations=False)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Validate

# COMMAND ----------

checkpoint_name = checkpoint_name
checkpoint_config = {
    "name": checkpoint_name,
    "config_version": 1.0,
    "class_name": "SimpleCheckpoint",
    "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
}

# COMMAND ----------

checkpoint = context.test_yaml_config(yaml.dump(checkpoint_config))

# COMMAND ----------

context.add_checkpoint(**checkpoint_config)

# COMMAND ----------

checkpoint_result = context.run_checkpoint(
    checkpoint_name=checkpoint_name,
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": suite_name,
        }
    ],
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Download

# COMMAND ----------

ge_doc_path = f"/{ge_path}/uncommitted/data_docs/local_site"
tmp_path = f"file:/tmp/ge_{area}_{process_group}"
from_tar_file = f"ge_{area}_{process_group}.tar.gz"
to_tar_file = f"ge_{area}_{process_group}_{stage}_{entity}.tar.gz"

dbutils.fs.rm(tmp_path, True)
dbutils.fs.rm(f"{tmp_path}/{from_tar_file}", True)

# COMMAND ----------

dbutils.fs.cp(ge_path, tmp_path, True)

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC tar -czvf /tmp/ge_area_processgroup.tar.gz /tmp/ge_area_processgroup

# COMMAND ----------

tmp_path = f"file:/tmp"
from_path = f"{tmp_path}/{from_tar_file}"
to_path = f"{root_path}/{to_tar_file}"
print(f"Copying from {from_path} to {to_path}")
dbutils.fs.cp(f"{tmp_path}/{from_tar_file}", f"{root_path}/{to_tar_file}", True)

# COMMAND ----------

displayHTML(f"""
<a href='https://{host}/files/{root_sub_path}/{to_tar_file}'>tar -xvf {to_tar_file}</a>
""")
