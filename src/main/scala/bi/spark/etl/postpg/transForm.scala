package bi.spark.etl.postpg
import bi.spark.etl.postpg.typeTrans.typeChang

/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/8/10 0010 上午 11:02.
  * Update Date Time:
  * see   用于接收每个表的字段信息
  */

object transForm {
  //给读入时,将表的原始字段名放到字典中
  def dataTrans(datadict:scala.collection.mutable.Map[String,Map[String,String]],df:org.apache.spark.sql.DataFrame):Unit={
    //获取dataframe的schema
    val df_schema = df.schema
    //把schema数组遍历为字典
    for(i <- df_schema){
      datadict(i.name) = Map[String,String]("name"->i.name,"alias"->i.name,"type"->i.dataType.toString)
    }
  }
  def postDict2Str(datadict:scala.collection.mutable.Map[String,Map[String,String]]):String={
    //获取最终字段名称的值
    val dict_values = datadict.values
    //形成一段字符串
    var api_str = ""
    //遍历每个值，形成api需要的字符串形式
    for(i <- dict_values){
      api_str += s"""{"name":"${i("name")}","alias":"${i("alias")}","type":"${typeChang(i("type"))}"},"""
    }
    //init取除最后一个元素外的所有元素
    s"""[${api_str.init}]"""
  }

}
