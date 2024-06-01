# Databricks notebook source
# MAGIC %pip install pydeequ

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
os.environ["SPARK_VERSION"] = "3.3"


# COMMAND ----------

from pyspark.sql import SparkSession, Row
import pydeequ

spark = (SparkSession
    .builder
    .config("spark.jars.packages", pydeequ.deequ_maven_coord)
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    .getOrCreate())

df = spark.sparkContext.parallelize([
            Row(a="foo", b=1, c=5),
            Row(a="bar", b=2, c=6),
            Row(a="baz", b=3, c=None)]).toDF()

# COMMAND ----------

from pydeequ.analyzers import *

analysisResult = (AnalysisRunner(spark) 
                    .onData(df) 
                    .addAnalyzer(Size()) 
                    .addAnalyzer(Completeness("b")) 
                    .run())

analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
display(analysisResult_df)


# COMMAND ----------

from pydeequ.profiles import *

result = ColumnProfilerRunner(spark) \
    .onData(df) \
    .run()

for col, profile in result.profiles.items():
    print(profile)

# COMMAND ----------

from pydeequ.suggestions import *

suggestionResult = ConstraintSuggestionRunner(spark) \
             .onData(df) \
             .addConstraintRule(DEFAULT()) \
             .run()

# Constraint Suggestions in JSON format
print(suggestionResult)

# COMMAND ----------

from pydeequ.checks import *
from pydeequ.verification import *

check = Check(spark, CheckLevel.Warning, "Review Check")

checkResult = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(
        check.hasSize(lambda x: x >= 3) \
        .hasMin("b", lambda x: x == 0) \
        .isComplete("c")  \
        .isUnique("a")  \
        .isContainedIn("a", ["foo", "bar", "baz"]) \
        .isNonNegative("b")) \
    .run()

checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
display(checkResult_df)

