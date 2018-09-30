package bi.spark.etl
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSON
import scala.util.parsing.json.JSONObject


/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/7/17 0017 下午 2:16.
  * Update Date Time:
  * see  传入一段json格式的字符串，返回一个字典
  * """{"key1":"value1","key2":"value2"}""   =>  {"key1":"value1","key2":"value2"}
  */

object jsonAnalyse {

  def regixJson(json:Option[Any]) =json match {
    case Some(map: Map[String, Any]) => map
  }

  def strToDict(str:String)={

  regixJson(JSON.parseFull(str))

  }

  //返回key:value[Any],
  def anyToDictAny(any:Any):Map[String,Any]={
      any.asInstanceOf[Map[String,Any]]
  }

  //返回key:value[String],
  def anyToDictStr(any:Any):Map[String,String]={
    any.asInstanceOf[Map[String,String]]
  }

  //返回key:value[List[String]],
  def anyToDictList(any:Any):Map[String,List[String]]={
    any.asInstanceOf[Map[String,List[String]]]
  }

  def strToDict(any:Any):Map[String,List[String]]={
    any.asInstanceOf[Map[String,List[String]]]
  }

  //把可能乱序的列表，通过position的x,y重新排序
  def sortDictToArray(mapkeys:scala.collection.mutable.Map[Map[String,Double],Map[String, Any]]):ArrayBuffer[Map[String,Any]]={
    //外部传入的，以Map("x"->1.0,"y"->1.0)->Map(source -> Map(type -> excel))，获取keys
    //即为Map("x"->1.0,"y"->1.0)的键
    val order_keys = mapkeys.keys
    //用以获取y值得一个列表
    val key_array = ArrayBuffer[Double]()
    //创建返回的列表，结果是有顺序的
    val sort_keylist = scala.collection.mutable.ArrayBuffer[Map[String,Any]]()
    //获取y的所有值
    for(y <- order_keys){key_array.append(y("y"))}
    //获取y最大值
    val max_y = key_array.max.asInstanceOf[Int]
    //从y->1开始遍历所有position的顺序
    for(i <- Array.range(1,max_y+1)){
      //用以递增的一个变量，判断x是否存在
      var x = 1
      for(w <- order_keys){
        for(j <- order_keys){
          //判断x->1,y->1是否存在，逐步获取顺序，并把这个顺序放到输出的列表中
          if(j.exists( _ == ("y",i)) && j.exists( _ == ("x",x))){
//            println(mapkeys(j))
            sort_keylist.append(mapkeys(j))
            //存在的话，x就递增
            x += 1
          }
        }
      }
    }
    sort_keylist
  }
}
