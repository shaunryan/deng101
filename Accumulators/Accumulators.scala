// Databricks notebook source
case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)

val flights = spark.read
  .parquet("dbfs:/databricks-datasets/definitive-guide/data/flight-data/parquet/2010-summary.parquet")
  .as[Flight]


// COMMAND ----------

// MAGIC %md 
// MAGIC ### Basic Accumulator

// COMMAND ----------


//Create the Accumulator

import org.apache.spark.util.LongAccumulator
//val accUnnamed = new LongAccumulator
//val acc = spark.sparkContext.register(accUnnamed)

//in 2 lines
//val accChina = new LongAccumulator
//spark.sparkContext.register(accChina, "China")

// in 1 lines
val accChina = spark.sparkContext.longAccumulator("China")

//define the accumulator function
def accChinaFunc(flight_row: Flight) = {
  val destination = flight_row.DEST_COUNTRY_NAME
  val origin = flight_row.ORIGIN_COUNTRY_NAME
  if (destination == "China")
  {
    accChina.add(flight_row.count.toLong)
  }
  if (origin == "China") {
    accChina.add(flight_row.count.toLong)
  }
}
//iterate the function over the rows
flights.foreach(flight_row => accChinaFunc(flight_row))

accChina.value

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Custom Accumulator

// COMMAND ----------

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.util.AccumulatorV2

val arr = ArrayBuffer[BigInt]()

class EvenAccumulator extends AccumulatorV2[BigInt, BigInt]
{
  private var num:BigInt = 0
  
  def reset(): Unit = 
  {
    this.num = 0
  }
  
  def add( intValue: BigInt): Unit = 
  { 
    if (intValue % 2 == 0) 
    { 
      this.num += intValue 
    } 
  } 
  
  def merge( other: AccumulatorV2[ BigInt, BigInt]): Unit = 
  { 
    this.num += other.value 
  } 
  
  def value(): BigInt = 
  { 
    this.num 
  } 
  
  def copy(): AccumulatorV2[ BigInt, BigInt] = 
  { 
    new EvenAccumulator 
  } 
  
  def isZero(): Boolean = 
  { 
    this.num == 0 
  } 
} 

val acc = new EvenAccumulator 
val newAcc = sc.register( acc, "evenAcc")

acc.value
flights.foreach(flight_row=> acc.add(flight_row.count))
acc.value
  


// COMMAND ----------

flights.first.count
