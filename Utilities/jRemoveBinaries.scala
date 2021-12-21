// Databricks notebook source
val jarVersion  = "gocogroup_dataenginerringcontrol_2_11_0_"

// COMMAND ----------

val j = dbutils.fs.ls("dbfs:/FileStore/jars/").filter(n=>n.name.contains(jarVersion)).map(p=>p.path)
j.foreach(println(_))

println("\n\n\n")


// COMMAND ----------

j.foreach(f => dbutils.fs.rm(f, false))

// COMMAND ----------

val j = dbutils.fs.ls("dbfs:/FileStore/jars/").filter(n=>n.name.contains(jarVersion)).map(p=>p.path)
