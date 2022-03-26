# Databricks notebook source
dbutils.fs.rm("/FileStore/data_profiles/ge", True)

# COMMAND ----------

# MAGIC %pip install great-expectations

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

root_directory = "/dbfs/FileStore/data_profiles/ge"

# COMMAND ----------

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=root_directory
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

my_spark_datasource_config = {
    "name": "insert_your_datasource_name_here",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        "insert_your_data_connector_name_here": {
            "module_name": "great_expectations.datasource.data_connector",
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": [
                "some_key_maybe_pipeline_stage",
                "some_other_key_maybe_run_id",
            ],
        }
    },
}

# COMMAND ----------

context.test_yaml_config(yaml.dump(my_spark_datasource_config))

# COMMAND ----------

context.add_datasource(**my_spark_datasource_config)

# COMMAND ----------

batch_request = RuntimeBatchRequest(
    datasource_name="insert_your_datasource_name_here",
    data_connector_name="insert_your_data_connector_name_here",
    data_asset_name="<YOUR_MEANGINGFUL_NAME>",  # This can be anything that identifies this data_asset for you
    batch_identifiers={
        "some_key_maybe_pipeline_stage": "prod",
        "some_other_key_maybe_run_id": f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={"batch_data": df},  # Your dataframe goes here
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Create Expectations

# COMMAND ----------

expectation_suite_name = "insert_your_expectation_suite_name_here"
context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
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

my_checkpoint_name = "insert_your_checkpoint_name_here"
checkpoint_config = {
    "name": my_checkpoint_name,
    "config_version": 1.0,
    "class_name": "SimpleCheckpoint",
    "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
}

# COMMAND ----------

my_checkpoint = context.test_yaml_config(yaml.dump(checkpoint_config))

# COMMAND ----------

context.add_checkpoint(**checkpoint_config)

# COMMAND ----------

checkpoint_result = context.run_checkpoint(
    checkpoint_name=my_checkpoint_name,
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
# MAGIC # Download

# COMMAND ----------

ge_path = "/FileStore/data_profiles/ge/uncommitted/data_docs/local_site"
tmp_path = "file:/tmp/ge"
dbutils.fs.rm("file:/tmp/ge", True)
dbutils.fs.rm("/FileStore/data_profiles/ge.tar.gz", True)

# COMMAND ----------

dbutils.fs.cp(ge_path, tmp_path, True)

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC tar -czvf /tmp/ge.tar.gz /tmp/ge

# COMMAND ----------

dbutils.fs.cp("file:/tmp/ge.tar.gz", "/FileStore/data_profiles/ge.tar.gz", True)

# COMMAND ----------

displayHTML("""
<a href='https://adb-8723178682651460.0.azuredatabricks.net/files/data_profiles/ge.tar.gz'>tar -xvf archive.tar.gz</a>
""")
