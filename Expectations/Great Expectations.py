# Databricks notebook source
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

root_directory = "/dbfs/great_expectations/"

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=root_directory
    ),
)
context = BaseDataContext(project_config=data_context_config)

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz")

display(df)

# COMMAND ----------

my_spark_datasource_config = """
name: insert_your_datasource_name_here
class_name: Datasource
execution_engine:
  class_name: SparkDFExecutionEngine
data_connectors:
  insert_your_data_connector_name_here:
    module_name: great_expectations.datasource.data_connector
    class_name: RuntimeDataConnector
    batch_identifiers:
      - some_key_maybe_pipeline_stage
      - some_other_key_maybe_run_id
"""

# COMMAND ----------

context.test_yaml_config(my_spark_datasource_config)

# COMMAND ----------

context.add_datasource(**yaml.load(my_spark_datasource_config))

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

validator.expect_column_values_to_not_be_null(column="passenger_count")

# COMMAND ----------

validator.expect_column_values_to_be_between(
    column="congestion_surcharge", min_value=0, max_value=1000
)

# COMMAND ----------

validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------

my_checkpoint_name = "insert_your_checkpoint_name_here"
my_checkpoint_config = f"""
name: {my_checkpoint_name}
config_version: 1.0
class_name: SimpleCheckpoint
run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
"""

# COMMAND ----------

my_checkpoint = context.test_yaml_config(my_checkpoint_config)

# COMMAND ----------

context.add_checkpoint(**yaml.load(my_checkpoint_config))

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

display()

# COMMAND ----------

# databricks fs cp -r dbfs:/great_expectations/uncommitted/data_docs/local_site/great_expectations/uncommitted/data_docs/local_site/
