// Databricks notebook source


// COMMAND ----------


import scala.io.Source
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.entity.StringEntity
import org.apache.http.entity.ContentType
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import com.google.gson.Gson

case class GetDistId(postcode: String)

object NewHttpJsonPostTest {
  
  // create our object as a json string
  val postcode = new GetDistId("BS159RH")
  val postcodeAsJson = new Gson().toJson(postcode) 
  
  val post = new HttpPost("https://apis2.awsprod.energylinx.com/v3.1/partner-resources/GOCOMPARETPI/dist-ids/?no_cache=1")
  //post.addHeader("Content-Type", "application/json")
  post.addHeader("Authorization", "Token ff7fee4f32914abdabe8354975e60490")
  
  //var postParameters = new ArrayList[NameValuePair]()
  //postParameters.add(new BasicNameValuePair("postcode", "BS159RH"))
  println(postcodeAsJson)

  val entity = new StringEntity(postcodeAsJson);
  entity.setContentType(ContentType.APPLICATION_JSON.getMimeType)
  post.setEntity(entity)
  post.setHeader("Content-Type", "application/json")
  post.setHeader("Authorization", "Token ff7fee4f32914abdabe8354975e60490")

  // send the post request
  val client = HttpClientBuilder.create.build
  val response = client.execute(post)
  println("--- HEADERS ---")
  response.getAllHeaders.foreach(arg => println(arg))
  
  println("Response Code : " + response.getStatusLine().getStatusCode())
  
  val reponseEntity = response.getEntity()
  var content = ""
  if (reponseEntity != null) {
    val inputStream = reponseEntity.getContent()
    content = Source.fromInputStream(inputStream).getLines.mkString
    inputStream.close
  }
  println(content)
  
}
NewHttpJsonPostTest


// COMMAND ----------

import java.io._
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import com.google.gson.Gson

//test outgoing
object NewHttpJsonPostTest {
  
  val post = new HttpGet("https://www.google.com")

  // send the post request
  val client = HttpClientBuilder.create.build
  val response = client.execute(post)
  println("--- HEADERS ---")
  response.getAllHeaders.foreach(arg => println(arg))
  
  println("Response Code : " + response.getStatusLine().getStatusCode())
}
NewHttpJsonPostTest


// COMMAND ----------


