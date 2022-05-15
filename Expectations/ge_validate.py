# Databricks notebook source
# MAGIC %pip install great-expectations==0.14.12

# COMMAND ----------

# dbutils.widgets.removeAll()
dbutils.widgets.text("Database", "jaffle_shop", "Database")
dbutils.widgets.text("Table", "orders", "Table")

# COMMAND ----------

db = dbutils.widgets.get("Database")
table = dbutils.widgets.get("Table")

# COMMAND ----------

import great_expectations as ge
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.render.renderer import *
from great_expectations.render.view import DefaultJinjaPageView

# COMMAND ----------

df = spark.sql(f"select * from {db}.{table}")

# COMMAND ----------

gdf = SparkDFDataset(df)

gdf.expect_column_values_to_be_of_type("ID", "IntegerType")
gdf.expect_column_values_to_not_be_null("ID")
validation_result = gdf.validate()

document_model = ValidationResultsPageRenderer().render(validation_result)
displayHTML(DefaultJinjaPageView().render(document_model))


# COMMAND ----------


results = []
# from datetime import datetime
# now = datetime.now()

runtime = validation_result.meta.get("run_id")
now = runtime.run_time

batch_id = validation_result.meta.get("batch_kwargs").get("ge_batch_id")

name = f"{db}.{table}.{batch_id}"
tests = validation_result.statistics["evaluated_expectations"]
test_failures = validation_result.statistics["unsuccessful_expectations"]
test_pct = validation_result.statistics["success_percent"]

print(f"name: {name}")
print(f"tests: {tests}")
print(f"failures: {test_failures}")
print(f"failures_pct: {test_pct}")


for er in validation_result.results:
  column = er["expectation_config"]["kwargs"]["column"]
  r = {
      "classname": f"{db}.{table}.{column}",
      "name": er["expectation_config"]["expectation_type"],
      "time": "0.00",
      "timestamp": now,
      "failure": er["exception_info"]["raised_exception"],
      "message": er["exception_info"]["exception_message"],
      "message": er["exception_info"]["exception_message"],
      "message": er["exception_info"]["exception_traceback"]
    }
  results.append(r)



from pyspark.sql.types import *

schema = StructType([
  StructField("classname", StringType(), True),
  StructField("name", StringType(), True),
  StructField('time', StringType(), True),
  StructField('timestamp', TimestampType(), True),
  StructField("failure", BooleanType(), True),
  StructField("message", StringType(), True),
  StructField("exception_traceback", StringType(), True)
])

dfr = spark.createDataFrame(results, schema)
display(dfr)


# COMMAND ----------

validation_result.to_dict

# COMMAND ----------

# <testsuites>
# <testsuite name="Test1SampleTests1-20220424105242" tests="2" file=".py" time="0.049" timestamp="2022-04-24T10:52:42" failures="1" errors="0" skipped="0">
# 		<testcase classname="Test1SampleTests1" name="tests_always_succeeds" time="0.000" timestamp="2022-04-24T10:52:42" />
# 		<testcase classname="Test1SampleTests1" name="test_always_fails" time="0.049" timestamp="2022-04-24T10:52:42">
# 			<failure type="AssertionError" message="False is not true">Traceback (most recent call last):AssertionError: False is not true
# </failure>
# 		</testcase>
# 	</testsuite>
# 	<testsuite name="Test1SampleTests2-20220424105242" tests="2" file=".py" time="0.049" timestamp="2022-04-24T10:52:42" failures="1" errors="0" skipped="0">
# 		<testcase classname="Test1SampleTests2" name="tests_always_succeeds" time="0.000" timestamp="2022-04-24T10:52:42" />
# 		<testcase classname="Test1SampleTests2" name="test_always_fails" time="0.049" timestamp="2022-04-24T10:52:42">
# 			<failure type="AssertionError" message="False is not true">Traceback (most recent call last):
#   File "&lt;command-835618480472936&gt;", line 6, in test_always_fails
#     self.assertTrue(False)
# AssertionError: False is not true
# </failure>
# 		</testcase>
# 	</testsuite>
