# Databricks notebook source
# MAGIC %sh
# MAGIC java -version

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://archive.apache.org/dist/kafka/3.2.0/kafka_2.12-3.2.0.tgz
# MAGIC ls -ltr ./

# COMMAND ----------

# MAGIC %sh
# MAGIC tar -xzf kafka_2.12-3.2.0.tgz

# COMMAND ----------

# MAGIC %sh
# MAGIC cd kafka_2.12-3.2.0/
# MAGIC bin/zookeeper-server-start.sh config/zookeeper.properties

# COMMAND ----------

# MAGIC %sh
# MAGIC cd kafka_2.12-3.2.0/
# MAGIC bin/kafka-server-start.sh config/server.properties
