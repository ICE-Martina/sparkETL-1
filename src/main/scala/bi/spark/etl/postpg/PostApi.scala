package bi.spark.etl.postpg

import java.util

import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair

/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/8/9 0009 下午 6:37.
  * Update Date Time:
  * http://192.168.15.51:8181/docs/bi_facade/144
  * see 传api的一种方式，接收model_id和一段字符串
  */

class PostApi(model_id:String,data:String) {
  private val url = "http://192.168.15.154:10086/callback/model/schema/create"
  private val client = HttpClients.createDefault()
  private val post = new HttpPost(url)
  private val list = new util.ArrayList[BasicNameValuePair]
  list.add(new BasicNameValuePair("model_id", model_id))
  //"""[{"name":"字段名","alias":"别名","type":"字段类型"}]"""
  list.add(new BasicNameValuePair("data",data ))

  private val uefEntity = new UrlEncodedFormEntity(list, "UTF-8")
  post.setEntity(uefEntity)
  private val httpResponse = client.execute(post)

  private val text = httpResponse.getEntity

  println(text)

}
