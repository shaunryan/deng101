// Databricks notebook source
import org.apache.log4j.{Level, Logger}

// COMMAND ----------

  val logger = Logger.getLogger("DataEngineeringControl")

  logger.setLevel(Level.INFO)
  val printEnabled = true

  def logInfo(message:String) =
  {
    if (printEnabled) println(message)
    logger.info(message)
  }

  def logError(message:String, throwable:Throwable) =
  {
    if (printEnabled) println(message)
    logger.error(message, throwable)
    throw new Exception(message)
  }

  def logError(message:String) =
  {
    if (printEnabled) println(message)
    logger.error(message)
    throw new Exception(message)
  }

// COMMAND ----------

try
{
  val l = 1/0
}
catch
{
  case e:Exception => logError("test logging", e)
}

// COMMAND ----------

logError("test logging")
