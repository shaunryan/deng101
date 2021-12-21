// Databricks notebook source
// MAGIC %run AzureDataFactory/Includes/SetupEnvironment

// COMMAND ----------

import java.util.Calendar
import java.text.SimpleDateFormat;



object Maintenance
{
  private val mountPoint = "/mnt/backup/prepared/"
  private val source = getDataLakeStorageAccount + "backup/deltalake"
  private val hivedw = "dbfs:/user/hive/warehouse/"
  private val format = new SimpleDateFormat("yyyyMMdd HHmmssmmm")
  
  
  def Connect(): Unit =
  {
    var scope = getAutomationScope
    val clientId = dbutils.secrets.get(scope = scope, key = "AdlsClientId")
    val credential = dbutils.secrets.get(scope = scope, key = "AdlsCredential")

    val configs = Map(
      "dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
      "dfs.adls.oauth2.client.id" -> clientId,
      "dfs.adls.oauth2.credential" -> credential,
      "dfs.adls.oauth2.refresh.url" -> "https://login.microsoftonline.com/4d13380f-b2dc-401e-ade8-03a67801c676/oauth2/token")

    dbutils.fs.mount(
      source = source,
      mountPoint = mountPoint,
      extraConfigs = configs)
  }
  
  def BackupDatabase(database:String): Unit = 
  {
    val backup = hivedw + database
    val datetime : String = format.format(Calendar.getInstance().getTime())
    val to = mountPoint + database + "/" + datetime
    dbutils.fs.mkdirs(to)
    dbutils.fs.cp(backup, to, true)
  }
  
  def BackupPurge(database:String, purgeRention:Integer)
  {
    val location = mountPoint + database + "/"
    val backups =  dbutils.fs.ls(location).sortWith((s,t) => format.parse(s.name.replace("/","")).after(format.parse(t.name.replace("/",""))) ).map(_.path)
    val purge = backups.drop(purgeRention)
    for(bkp <- purge)
    {
      dbutils.fs.rm(bkp, true)
    }
  }
  
  
  def Disconnect()
  {
    dbutils.fs.unmount(mountPoint)
  }
  
}
