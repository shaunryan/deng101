// Databricks notebook source
import java.text.SimpleDateFormat

val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
val rundate = format.parse("2013-07-06")

val yearFormat = new SimpleDateFormat("yyyy")
val monthFormat = new SimpleDateFormat("MM")
val dayFormat = new SimpleDateFormat("dd")

val year = yearFormat.format(rundate)
val month = monthFormat.format(rundate)
val day = dayFormat.format(rundate)

print(year)
print(month)
print(day)

// COMMAND ----------


import java.time.LocalDate 
import java.time.format.DateTimeFormatter

//val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
//var fromdate = format.parse("2013-01-01")
//val todate = format.parse("2013-01-10")
//val days = todate.toEpochDay() - fromdate.toEpochDay()

val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
var fromDate = LocalDate.parse("2013-01-01", formatter)
val toDate = LocalDate.parse("2013-01-10", formatter)


while (toDate.toEpochDay() - fromDate.toEpochDay() > -1)
{

  println(formatter.format(fromDate));
  fromDate = fromDate.plusDays(1)
}


