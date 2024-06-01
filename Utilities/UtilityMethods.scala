// Databricks notebook source
// MAGIC %python
// MAGIC # ****************************************************************************
// MAGIC # Utility method to count & print the number of records in each partition.
// MAGIC # ****************************************************************************
// MAGIC
// MAGIC def printRecordsPerPartition(df):
// MAGIC   print("Per-Partition Counts:")
// MAGIC   def countInPartition(iterator): yield __builtin__.sum(1 for _ in iterator)
// MAGIC   results = (df.rdd                   # Convert to an RDD
// MAGIC     .mapPartitions(countInPartition)  # For each partition, count
// MAGIC     .collect()                        # Return the counts to the driver
// MAGIC   )
// MAGIC   for result in results: print("* " + str(result))
// MAGIC   
// MAGIC # ****************************************************************************
// MAGIC # Utility to count the number of files in and size of a directory
// MAGIC # ****************************************************************************
// MAGIC
// MAGIC def computeFileStats(path):
// MAGIC   bytes = 0
// MAGIC   count = 0
// MAGIC
// MAGIC   files = dbutils.fs.ls(path)
// MAGIC   
// MAGIC   while (len(files) > 0):
// MAGIC     fileInfo = files.pop(0)
// MAGIC     if (fileInfo.isDir() == False):               # isDir() is a method on the fileInfo object
// MAGIC       count += 1
// MAGIC       bytes += fileInfo.size                      # size is a parameter on the fileInfo object
// MAGIC     else:
// MAGIC       files.extend(dbutils.fs.ls(fileInfo.path))  # append multiple object to files
// MAGIC       
// MAGIC   return (count, bytes)
// MAGIC
// MAGIC # ****************************************************************************
// MAGIC # Utility method to cache a table with a specific name
// MAGIC # ****************************************************************************
// MAGIC
// MAGIC def cacheAs(df, name, level):
// MAGIC   print("WARNING: The PySpark API currently does not allow specification of the storage level - using MEMORY-ONLY")
// MAGIC   
// MAGIC   try: spark.catalog.uncacheTable(name)
// MAGIC   except AnalysisException: None
// MAGIC   
// MAGIC   df.createOrReplaceTempView(name)
// MAGIC   spark.catalog.cacheTable(name, level)
// MAGIC   return df
// MAGIC
// MAGIC
// MAGIC # ****************************************************************************
// MAGIC # Simplified benchmark of count()
// MAGIC # ****************************************************************************
// MAGIC
// MAGIC def benchmarkCount(func):
// MAGIC   import time
// MAGIC   start = float(time.time() * 1000)                    # Start the clock
// MAGIC   df = func()
// MAGIC   total = df.count()                                   # Count the records
// MAGIC   duration = float(time.time() * 1000) - start         # Stop the clock
// MAGIC   return (df, total, duration)
// MAGIC
// MAGIC None

// COMMAND ----------


// ****************************************************************************
// Utility method to count & print the number of records in each partition.
// ****************************************************************************

def printRecordsPerPartition(df:org.apache.spark.sql.Dataset[Row]):Unit = {
  // import org.apache.spark.sql.functions._
  println("Per-Partition Counts:")
  val results = df.rdd                                   // Convert to an RDD
    .mapPartitions(it => Array(it.size).iterator, true)  // For each partition, count
    .collect()                                           // Return the counts to the driver

  results.foreach(x => println("* " + x))
}

// ****************************************************************************
// Utility to count the number of files in and size of a directory
// ****************************************************************************

def computeFileStats(path:String):(Long,Long) = {
  var bytes = 0L
  var count = 0L

  import scala.collection.mutable.ArrayBuffer
  var files=ArrayBuffer(dbutils.fs.ls(path):_ *)

  while (files.isEmpty == false) {
    val fileInfo = files.remove(0)
    if (fileInfo.isDir == false) {
      count += 1
      bytes += fileInfo.size
    } else {
      files.append(dbutils.fs.ls(fileInfo.path):_ *)
    }
  }
  (count, bytes)
}

// ****************************************************************************
// Utility method to cache a table with a specific name
// ****************************************************************************

def cacheAs(df:org.apache.spark.sql.DataFrame, name:String, level:org.apache.spark.storage.StorageLevel):org.apache.spark.sql.DataFrame = {
  try spark.catalog.uncacheTable(name)
  catch { case _: org.apache.spark.sql.AnalysisException => () }
  
  df.createOrReplaceTempView(name)
  spark.catalog.cacheTable(name, level)
  return df
}

// ****************************************************************************
// Simplified benchmark of count()
// ****************************************************************************

def benchmarkCount(func:() => org.apache.spark.sql.DataFrame):(org.apache.spark.sql.DataFrame, Long, Long) = {
  val start = System.currentTimeMillis            // Start the clock
  val df = func()                                 // Get our lambda
  val total = df.count()                          // Count the records
  val duration = System.currentTimeMillis - start // Stop the clock
  (df, total, duration)
}

// ****************************************************************************
// Benchmarking and cache tracking tool
// ****************************************************************************

case class JobResults[T](runtime:Long, duration:Long, cacheSize:Long, maxCacheBefore:Long, remCacheBefore:Long, maxCacheAfter:Long, remCacheAfter:Long, result:T) {
  def printTime():Unit = {
    if (runtime < 1000)                 println(f"Runtime:  ${runtime}%,d ms")
    else if (runtime < 60 * 1000)       println(f"Runtime:  ${runtime/1000.0}%,.2f sec")
    else if (runtime < 60 * 60 * 1000)  println(f"Runtime:  ${runtime/1000.0/60.0}%,.2f min")
    else                                println(f"Runtime:  ${runtime/1000.0/60.0/60.0}%,.2f hr")
    
    if (duration < 1000)                println(f"All Jobs: ${duration}%,d ms")
    else if (duration < 60 * 1000)      println(f"All Jobs: ${duration/1000.0}%,.2f sec")
    else if (duration < 60 * 60 * 1000) println(f"All Jobs: ${duration/1000.0/60.0}%,.2f min")
    else                                println(f"Job Dur: ${duration/1000.0/60.0/60.0}%,.2f hr")
  }
  def printCache():Unit = {
    if (Math.abs(cacheSize) < 1024)                    println(f"Cached:   ${cacheSize}%,d bytes")
    else if (Math.abs(cacheSize) < 1024 * 1024)        println(f"Cached:   ${cacheSize/1024.0}%,.3f KB")
    else if (Math.abs(cacheSize) < 1024 * 1024 * 1024) println(f"Cached:   ${cacheSize/1024.0/1024.0}%,.3f MB")
    else                                               println(f"Cached:   ${cacheSize/1024.0/1024.0/1024.0}%,.3f GB")
    
    println(f"Before:   ${remCacheBefore / 1024.0 / 1024.0}%,.3f / ${maxCacheBefore / 1024.0 / 1024.0}%,.3f MB / ${100.0*remCacheBefore/maxCacheBefore}%.2f%%")
    println(f"After:    ${remCacheAfter / 1024.0 / 1024.0}%,.3f / ${maxCacheAfter / 1024.0 / 1024.0}%,.3f MB / ${100.0*remCacheAfter/maxCacheAfter}%.2f%%")
  }
  def print():Unit = {
    printTime()
    printCache()
  }
}

case class Node(driver:Boolean, executor:Boolean, address:String, maximum:Long, available:Long) {
  def this(address:String, maximum:Long, available:Long) = this(address.contains("-"), !address.contains("-"), address, maximum, available)
}

class Tracker() extends org.apache.spark.scheduler.SparkListener() {
  
  sc.addSparkListener(this)
  
  val jobStarts = scala.collection.mutable.Map[Int,Long]()
  val jobEnds = scala.collection.mutable.Map[Int,Long]()
  
  def track[T](func:() => T):JobResults[T] = {
    jobEnds.clear()
    jobStarts.clear()

    val executorsBefore = sc.getExecutorMemoryStatus.map(x => new Node(x._1, x._2._1, x._2._2)).filter(_.executor)
    val maxCacheBefore = executorsBefore.map(_.maximum).sum
    val remCacheBefore = executorsBefore.map(_.available).sum
    
    val start = System.currentTimeMillis()
    val result = func()
    val runtime = System.currentTimeMillis() - start
    
    Thread.sleep(1000) // give it a second to catch up

    val executorsAfter = sc.getExecutorMemoryStatus.map(x => new Node(x._1, x._2._1, x._2._2)).filter(_.executor)
    val maxCacheAfter = executorsAfter.map(_.maximum).sum
    val remCacheAfter = executorsAfter.map(_.available).sum

    var duration = 0L
    
    for ((jobId, startAt) <- jobStarts) {
      assert(jobEnds.keySet.exists(_ == jobId), s"A conclusion for Job ID $jobId was not found.") 
      duration += jobEnds(jobId) - startAt
    }
    JobResults(runtime, duration, remCacheBefore-remCacheAfter, maxCacheBefore, remCacheBefore, maxCacheAfter, remCacheAfter, result)
  }
  override def onJobStart(jobStart: org.apache.spark.scheduler.SparkListenerJobStart):Unit = jobStarts.put(jobStart.jobId, jobStart.time)
  override def onJobEnd(jobEnd: org.apache.spark.scheduler.SparkListenerJobEnd): Unit = jobEnds.put(jobEnd.jobId, jobEnd.time)
}

val tracker = new Tracker()


displayHTML("""
<div>Declared various utility methods:</div>
<li>Declared <b style="color:green">printRecordsPerPartition(<i>df:DataFrame</i>)</b> for diagnostics</li>
<li>Declared <b style="color:green">computeFileStats(<i>path:String</i>)</b> returns <b style="color:green">(count:Long, bytes:Long)</b> for diagnostics</li>
<li>Declared <b style="color:green">tracker</b> for benchmarking</li>
<li>Declared <b style="color:green">cacheAs(<i>df:DataFrame, name:String, level:StorageLevel</i>)</b> for better debugging</li>
<li>Declared <b style="color:green">benchmarkCount(<i>lambda:DataFrame</i>)</b> returns <b style="color:green">(df:DataFrame, total:Long, duration:Long)</b> for diagnostics</li>
<br/>
<div>All done!</div>
""")
