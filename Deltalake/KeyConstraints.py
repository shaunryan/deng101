# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC select * from samples.nyctaxi.trips

# COMMAND ----------

# MAGIC %sql
# MAGIC Create schema samples.deltademo

# COMMAND ----------

# MAGIC  %sql
# MAGIC  CREATE TABLE main.deltademo.persons(first_name STRING NOT NULL, last_name STRING NOT NULL, nickname STRING,
# MAGIC                        CONSTRAINT persons_pk PRIMARY KEY(first_name, last_name));

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Create a table with a primary key
# MAGIC CREATE TABLE persons(first_name STRING NOT NULL, last_name STRING NOT NULL, nickname STRING,
# MAGIC                        CONSTRAINT persons_pk PRIMARY KEY(first_name, last_name));
# MAGIC 
# MAGIC -- create a table with a foreign key
# MAGIC > CREATE TABLE pets(name STRING, owner_first_name STRING, owner_last_name STRING,
# MAGIC                     CONSTRAINT pets_persons_fk FOREIGN KEY (owner_first_name, owner_last_name) REFERENCES persons);
# MAGIC 
# MAGIC -- Create a table with a single column primary key and system generated name
# MAGIC > CREATE TABLE customers(customerid STRING NOT NULL PRIMARY KEY, name STRING);
# MAGIC 
# MAGIC -- Create a table with a names single column primary key and a named single column foreign key
# MAGIC > CREATE TABLE orders(orderid BIGINT NOT NULL CONSTRAINT orders_pk PRIMARY KEY,
# MAGIC                       customerid STRING CONSTRAINT orders_customers_fk REFERENCES customers);
