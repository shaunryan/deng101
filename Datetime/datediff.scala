// Databricks notebook source
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit
import java.util.Calendar


def dateDiffDay(first:Date, latest:Date=Calendar.getInstance().getTime(), datepart:TimeUnit=TimeUnit.DAYS) =
{
  val diffInMillies = Math.abs(first.getTime() - latest.getTime())
 datepart.convert(diffInMillies, TimeUnit.MILLISECONDS)
}

// COMMAND ----------

val dateformat = new SimpleDateFormat("yyyy-MM-dd")

val dte = dateformat.parse("2019-11-01")

//date difference in days between date and now
dateDiffDay(dte)

// COMMAND ----------

//date difference in hours between date and now
dateDiffDay(first=dte, datepart=TimeUnit.HOURS)

// COMMAND ----------

//date difference in days between now and another date
val lastdate = dateformat.parse("2019-11-02")
dateDiffDay(dte, lastdate)

// COMMAND ----------

//date difference in hours between now and another date
val lastdate = dateformat.parse("2019-11-02")
dateDiffDay(dte, lastdate, TimeUnit.HOURS)
