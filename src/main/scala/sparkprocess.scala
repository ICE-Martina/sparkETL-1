import java.util.concurrent.TimeUnit

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import bi.spark.etl._
import bi.spark.etl.postpg.PostApi
import bi.spark.etl.postpg.transForm.postDict2Str
import bi.spark.etl.postpg.PostApi
import com.alibaba.fastjson.{JSON, JSONArray}

import scala.collection.mutable
import scala.io.{BufferedSource, Source}




/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/7/17 0017 上午 10:39.
  * Update Date Time:
  * see
  */

object sparkprocess {
  def main(args: Array[String]): Unit = {

//    System.setProperty("hadoop.home.dir", "D:\\download\\分布式工具\\hadoop\\hadoop-common-2.2.0-bin")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val model_id:String = args(0)

    val sparksql = SparkSession.builder().appName(s"SparkETL-model_id-${model_id}").getOrCreate()

//    val sparksql = SparkSession.builder().appName("SparkETL").master("local[2]").getOrCreate()

    val sc = sparksql.sparkContext

    //创建一个空字典，接收所有需要创建的表，整个程序以这个字典，共享所有dataframe
    val dflist = scala.collection.mutable.Map[String,DataFrame]()

   //最后返回给api的名称对应
    /*
    [{
        "name":"字段名",
        "alias":"别名",
        "type":"字段类型"
    }]
     */
    val api_data_dict = scala.collection.mutable.Map[String,Map[String,String]]()

    //控制整个过程dataframe的传递
    val pro = new DataProcess(sparksql,dflist)

//    //测试json
//======================================================================>
//    val jsontext:BufferedSource = Source.fromFile("F:\\qingmu\\2018\\7month\\bicodev2art\\ETLjsonDemo\\test1.txt")
////
//    var strjson = ""
//    for(i <- jsontext.getLines()){
//      strjson += i.replaceAll("\t","").replaceAll(" ","")
//    }
//
//    //把传入json转为字典
//    val testjson = jsonAnalyse.strToDict(strjson)
//    println(testjson)
//    val config = testjson("config").asInstanceOf[List[Map[String,Any]]]
//    val model_id =  testjson("model_id").toString
//    //======================================================================>
//
//
//    val pro = new DataProcess(sparksql,dflist)
//======================================================================>
    //实际运行
    //读入{"model_id":"",config:[]}
//    val arg:String = args(0)
//    //根据输入的model_id获取hdfs上的json配置文件
//    val json_info = sc.textFile(s"hdfs://ns1/bi/model/${arg1}.json").first()
////    val json_info = hdfs_text(0)
//    println(json_info)

    //本地测试
    //读入{"model_id":"",config:[]}
//    val json_file:BufferedSource = Source.fromFile(s"F:\\qingmu\\2018\\7month\\bicodev2art\\ETLjsonDemo\\test.json","utf-8")
////    //特获取第一行
//    val json_info = json_file.getLines.toList(0)
//
//    val testjson = jsonAnalyse.strToDict(json_info)
//    val config = testjson("config").asInstanceOf[List[Map[String,Any]]]
//    val model_id =  testjson("model_id").toString


    //======================================================================>
    //直接读入[{},{},{}]的数据
//    val json_file:BufferedSource = Source.fromFile("D:\\F\\qingmu\\2018\\7month\\bicodev2art\\ETLjsonDemo\\test3.txt","utf-8")
//    val json_info = json_file.getLines.toList(0)
//    println(json_info)
//    val testjson = JSON.parseArray(json_info)
//    val json_size = testjson.size()
//    for(i <- Array.range(0,json_size)){
//      val each_config= jsonAnalyse.strToDict(testjson.getJSONObject(i).toString)
//      pro.act(each_config,api_data_dict)
//    }

    //实际运行
    val json_info = sc.textFile(s"hdfs://ns1/bi/model/${model_id}.json").first()
    //把[{},{},{}]解释回数组元素为字典字符串的形式
    val testjson = JSON.parseArray(json_info)
    //获取数组长度
    val json_size = testjson.size()
    //由于不能保证列表的顺序，所以要按照position再排序依次
    //把position拿出，组成{"position":[原对应内容]}
    val map_josn = mutable.Map[Map[String,Double],Map[String, Any]]()
    for(i <- Array.range(0,json_size)){
      //把字典型的字符串转为字典
      val each_config= jsonAnalyse.strToDict(testjson.getJSONObject(i).toString)
      //遍历每个数组，对应的position与内容
      val each_mapkey = each_config("position").asInstanceOf[Map[String,Double]]
      map_josn(each_mapkey) = each_config
//      println(each_config("position"),map_josn(each_mapkey))
//      pro.act(each_config,api_data_dict)
    }
    //======================================================================>
    //把可能乱序的列表，通过position的x,y重新排序
    val jsonsort = jsonAnalyse.sortDictToArray(map_josn)
//    val json_size2 = testjson.size()
    println(jsonsort.length)
    for(i <- jsonsort){
      println(i)
    }
    for(each <- jsonsort){
      println(each)
      pro.act(each,api_data_dict)
    }





//    for(each_config <- config){
//      //循环执行config的内容
//      pro.act(each_config,api_data_dict)
//    }
//
//    //调用postDict2Str，把列名对应的映射转成api post的字符格式
    val api_data = postDict2Str(api_data_dict)
    println(api_data)



    //实例一个调用返回值的api接口，把表结构内容传出
    //http://192.168.15.51:8181/docs/bi_facade/144
    new PostApi(model_id,api_data)

//    TimeUnit.MINUTES.sleep(40)
  }
}
