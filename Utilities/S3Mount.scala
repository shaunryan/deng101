// Databricks notebook source
// MAGIC %run AzureDataFactory/Includes/SetupEnvironment

// COMMAND ----------

// MAGIC %scala
// MAGIC import java.text.SimpleDateFormat
// MAGIC import java.util.Date
// MAGIC import java.util.concurrent.TimeUnit
// MAGIC 
// MAGIC object S3Manager
// MAGIC {
// MAGIC   
// MAGIC   def wrapHtml(rows:Seq[String]) =
// MAGIC   {
// MAGIC     var html = """
// MAGIC     <p>
// MAGIC       <strong>Clearing AWS source data</strong>
// MAGIC     </p>
// MAGIC     """
// MAGIC 
// MAGIC     var tr = ""
// MAGIC     for(r <- rows)
// MAGIC     {
// MAGIC       tr = tr + "<tr><td>" + r + "</td></tr>"
// MAGIC     }
// MAGIC 
// MAGIC     html = html + """
// MAGIC          <!DOCTYPE html>
// MAGIC          <html>
// MAGIC          <head>
// MAGIC          <style>
// MAGIC          body {
// MAGIC            font-family: 'Verdana';
// MAGIC            font-size: x-small;
// MAGIC          }
// MAGIC          table {
// MAGIC            border-collapse: collapse;
// MAGIC 
// MAGIC            font-size: small;
// MAGIC 
// MAGIC          }
// MAGIC          th {
// MAGIC            background-color: #4CAF50;
// MAGIC            color: white;
// MAGIC          }
// MAGIC          th, td {
// MAGIC            text-align: left;
// MAGIC            padding: 8px;
// MAGIC          }
// MAGIC 
// MAGIC          tr:nth-child(even) {background-color: #f2f2f2;}
// MAGIC          </style>
// MAGIC          </head>
// MAGIC          <body>
// MAGIC 
// MAGIC          <div style='overflow-x:auto;'>
// MAGIC            <table>
// MAGIC              <tr>
// MAGIC              <th>File Path</th>
// MAGIC              </tr>"""
// MAGIC 
// MAGIC     html = html + tr
// MAGIC 
// MAGIC     html + """</table>
// MAGIC          </div>
// MAGIC 
// MAGIC          </body>
// MAGIC          </html>"""
// MAGIC   }
// MAGIC 
// MAGIC   def unmountS3(name:String) =
// MAGIC   {
// MAGIC     if (dbutils.fs.mounts().map(_.mountPoint).contains(name)) dbutils.fs.unmount(name)
// MAGIC   }
// MAGIC   
// MAGIC   def mountS3(key:String, name:String) =
// MAGIC   {
// MAGIC     val accessKey = dbutils.secrets.get(scope = getAutomationScope, key)
// MAGIC     val mountName = s"/mnt/${name}"
// MAGIC     if (dbutils.fs.mounts().map(_.mountPoint).contains(mountName)) dbutils.fs.unmount(mountName)
// MAGIC     dbutils.fs.mount(accessKey, mountName)
// MAGIC     mountName + "/"
// MAGIC   }
// MAGIC 
// MAGIC   def dateDiffDay(first:Date, latest:Date) =
// MAGIC   {
// MAGIC     val diffInMillies = Math.abs(first.getTime() - latest.getTime())
// MAGIC     TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS)
// MAGIC   }
// MAGIC 
// MAGIC   def clearData(format:String, s3Key:String, mountName:String, retentionThresholdDays:Int, commit:Boolean=false)
// MAGIC   {
// MAGIC     val mountPath = mountS3(s3Key, mountName)
// MAGIC     val dateFormat = new SimpleDateFormat(format)
// MAGIC     val folders = dbutils.fs.ls(mountPath).map(folder => dateFormat.parse(folder.name.replace("/", "")))
// MAGIC     val remove = folders.filter(dateDiffDay(_, folders.max) > retentionThresholdDays-1).map(date => s"${mountPath}${dateFormat.format(date)}/")
// MAGIC 
// MAGIC     if (commit) remove.par.foreach(dbutils.fs.rm(_, true))
// MAGIC     unmountS3(mountPath)
// MAGIC     displayHTML(wrapHtml(remove))
// MAGIC   }
// MAGIC }

// COMMAND ----------

import com.gocogroup.metadata._

// COMMAND ----------

val c = Configuration()
val rpt = S3Manager.clearData("yyyy-MM-dd", c.automationScope, "ElxS3DataFeed01Bucket", "elx-s3-data-feed-01", 50, false)
displayHTML(rpt)
