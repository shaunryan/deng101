# Databricks notebook source
df = spark.read.format("json") \
.load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json") 

sample = df.sample(fraction=0.5, withReplacement=True, seed=1)

display(sample)
