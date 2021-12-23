# Databricks notebook source
from utilities import AppConfig

app_config = AppConfig(dbutils, spark)

app_config.help()


