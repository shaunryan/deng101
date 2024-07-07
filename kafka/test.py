# Databricks notebook source
# MAGIC %sh
# MAGIC cd kafka_2.12-3.2.0
# MAGIC bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic NewTopic

# COMMAND ----------

# MAGIC %sh
# MAGIC cd kafka_2.12-3.2.0
# MAGIC bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# COMMAND ----------

# MAGIC %sh
# MAGIC cd kafka_2.12-3.2.0echo "Third Msg" | bin/kafka-console-producer.sh --topic NewTopic --bootstrap-server localhost:9092

# COMMAND ----------

# MAGIC %sh
# MAGIC cd kafka_2.12-3.2.0
# MAGIC bin/kafka-console-consumer.sh --topic startevents --from-beginning --bootstrap-server localhost:9092
