// Databricks notebook source
import org.apache.http.entity.StringEntity
import org.apache.http.entity.ContentType
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import com.google.gson.Gson
 
    
    val endpoint = "https://apis2.awsprod.energylinx.com/v3.3/partner-resources/GOCOMPARETPI-BLETCHLEY/supplier-details/"
    val get = new HttpGet(endpoint)
    val entity = new StringEntity("")
    
    entity.setContentType(ContentType.APPLICATION_JSON.getMimeType)
    //get.setEntity(entity)
    get.setHeader("Content-Type", "application/json")
    
    // send the alert request to webservice app the sends the email.
    val client = HttpClientBuilder.create.build
    val response = client.execute(get)
    //println("Email Response Code : " + response.getStatusLine().getStatusCode())
    response.close
    response.getStatusLine().toString()


// COMMAND ----------

response.toString()

// COMMAND ----------

val client = HttpClientBuilder.create.build
val request = new HttpGet("https://apis2.awsprod.energylinx.com/v3.3/partner-resources/GOCOMPARETPI-BLETCHLEY/supplier-details/")
val response = client.execute(request)

// COMMAND ----------


import org.apache.commons.io.IOUtils
import org.apache.commons.io.Charsets
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import spark.implicits._

val client = HttpClientBuilder.create.build
val get = new HttpGet("https://apis2.awsprod.energylinx.com/v3.3/partner-resources/GOCOMPARETPI-BLETCHLEY/supplier-details/")
get.setHeader("Content-Type", "application/json")
val inputStream = client.execute(get).getEntity.getContent
//val text = IOUtils.toString(inputStream, Charsets.UTF_8.name());
val df = spark.read.json(Seq(IOUtils.toString(inputStream, Charsets.UTF_8.name())).toDS)



// COMMAND ----------

display(df)
