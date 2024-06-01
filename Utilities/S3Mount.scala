// Databricks notebook source
// MAGIC %run AzureDataFactory/Includes/SetupEnvironment

// COMMAND ----------

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

object S3Manager
{
  
  def wrapHtml(rows:Seq[String]) =
  {
    var html = """
    <p>
      <strong>Clearing AWS source data</strong>
    </p>
    """

    var tr = ""
    for(r <- rows)
    {
      tr = tr + "<tr><td>" + r + "</td></tr>"
    }

    html = html + """
         <!DOCTYPE html>
         <html>
         <head>
         <style>
         body {
           font-family: 'Verdana';
           font-size: x-small;
         }
         table {
           border-collapse: collapse;

           font-size: small;

         }
         th {
           background-color: #4CAF50;
           color: white;
         }
         th, td {
           text-align: left;
           padding: 8px;
         }

         tr:nth-child(even) {background-color: #f2f2f2;}
         </style>
         </head>
         <body>

         <div style='overflow-x:auto;'>
           <table>
             <tr>
             <th>File Path</th>
             </tr>"""

    html = html + tr

    html + """</table>
         </div>

         </body>
         </html>"""
  }

  def unmountS3(name:String) =
  {
    if (dbutils.fs.mounts().map(_.mountPoint).contains(name)) dbutils.fs.unmount(name)
  }
  
  def mountS3(key:String, name:String) =
  {
    val accessKey = dbutils.secrets.get(scope = getAutomationScope, key)
    val mountName = s"/mnt/${name}"
    if (dbutils.fs.mounts().map(_.mountPoint).contains(mountName)) dbutils.fs.unmount(mountName)
    dbutils.fs.mount(accessKey, mountName)
    mountName + "/"
  }

  def dateDiffDay(first:Date, latest:Date) =
  {
    val diffInMillies = Math.abs(first.getTime() - latest.getTime())
    TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS)
  }

  def clearData(format:String, s3Key:String, mountName:String, retentionThresholdDays:Int, commit:Boolean=false)
  {
    val mountPath = mountS3(s3Key, mountName)
    val dateFormat = new SimpleDateFormat(format)
    val folders = dbutils.fs.ls(mountPath).map(folder => dateFormat.parse(folder.name.replace("/", "")))
    val remove = folders.filter(dateDiffDay(_, folders.max) > retentionThresholdDays-1).map(date => s"${mountPath}${dateFormat.format(date)}/")

    if (commit) remove.par.foreach(dbutils.fs.rm(_, true))
    unmountS3(mountPath)
    displayHTML(wrapHtml(remove))
  }
}

// COMMAND ----------

import com.gocogroup.metadata._

// COMMAND ----------

val c = Configuration()
val rpt = S3Manager.clearData("yyyy-MM-dd", c.automationScope, "ElxS3DataFeed01Bucket", "elx-s3-data-feed-01", 50, false)
displayHTML(rpt)
