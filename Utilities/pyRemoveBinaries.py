# Databricks notebook source
def containsWheel(path:str, containsInName:str):
  
  listWheels = dbutils.fs.ls(path)
  wheels = [i.path for i in listWheels if name.lower() in i.name.lower()]
  
  if len(wheels) > 0:
    return True
  else:
    return False
  
  
def getBinaryPaths(containsInName:str):
  
  listDirs = dbutils.fs.ls("dbfs:/FileStore/jars/")
  result = [i.path for i in listDirs if containsWheel(i.path, containsInName)]
  
  return result
  
  
def deleteBinaries(containsInName:str):
  
  listDirs = getBinaryPaths(containsInName)
  result = [dbutils.fs.rm(i, True) for i in listDirs]
  
  return result
  


# COMMAND ----------

name  = "fathom"

getBinaryPaths(name)


# COMMAND ----------

deleteBinaries(name)
