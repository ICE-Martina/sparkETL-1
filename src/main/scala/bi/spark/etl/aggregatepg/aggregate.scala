package bi.spark.etl.aggregatepg

import bi.spark.etl.jsonAnalyse
import org.apache.spark.sql
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

import scala.collection.mutable.MutableList

/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/7/19 0019 下午 3:33.
  * Update Date Time:
  * see
  */

object aggregate {
  def aggregate(sparksql:SparkSession,df_list:scala.collection.mutable.Map[String,org.apache.spark.sql.DataFrame],metajson: Map[String, Any],postdata:scala.collection.mutable.Map[String,Map[String,String]])={

    //获取position，并转为key:value[Any]
    val position = jsonAnalyse.anyToDictAny(metajson("position"))

    //获取operation，并转为key:value[Any]
    val operation = jsonAnalyse.anyToDictAny(metajson("operation"))

    //获取operation的parms，并转为key:value[Any]
    val operation_parms = operation("parms").asInstanceOf[Map[String,Any]]

    //数据源
    val source = metajson("source").asInstanceOf[List[String]](0)

    //获取最外层temp_name
    val temp_name = metajson("temp_name").toString

    //获取grouped_fields的列表值
    val grouped_fields = operation_parms("grouped_fields").asInstanceOf[List[Map[String,String]]]

    //gourpBy所需要的列名
    var groupfield = MutableList[Column]()
    //把需要gourpBy的列整为长参数
    grouped_fields.foreach( c => groupfield += col(c("grouped_dim")) )

    //获取agg_fields的列表值
    val agg_fields = operation_parms("agg_fields").asInstanceOf[List[Map[String,String]]]
    //agg所需要的参数
    var aggfields = scala.collection.mutable.MutableList[(String,String)]()
    //把agg所需的参数整为长参数
    agg_fields.foreach( i => aggfields += ((i("agg_dim"),i("agg_func"))))

//    var tmptable:sql.DataFrame = null
//
    val aggfile_lenth = aggfields.length
//    //判断agg_fields长度，选择计算的方式，由于agg传参问题，所以需要判断
//    if(aggfile_lenth <=1){
//      tmptable = df_list(source).groupBy(groupfield:_*).agg(aggfields.head)
//    }
//    else{
//      tmptable = df_list(source).groupBy(groupfield:_*).agg(aggfields.head,aggfields.tail:_*)
//    }
    var tmptable = df_list(source).groupBy(groupfield:_*).agg(aggfields.head,aggfields.tail:_*)

    tmptable.show()
    //聚合时，计算一次会新增一列，这里按照计算的函数个数，获取列名的个数（从右边取）
    val oldnames = tmptable.columns.takeRight(aggfile_lenth)
    println(oldnames)

    //剔除groupby列，新增的agg列从0开始索引，互相对应列名和json里agg_fields索引下对应的agg_new新列名
    Array.range(0,aggfile_lenth).foreach( j => {
      //获取agg层级的agg_new和agg_name的值，在修改列名的同时，修改返回给api的字典内容
      val agg_new = agg_fields(j)("agg_new")
//      val agg_name = agg_fields(j)("agg_new")
      tmptable = tmptable.withColumnRenamed(oldnames(j), agg_new)
      println(agg_new)
      tmptable.show()
      //获取新增列的schema
      val schema = tmptable.schema
      val schema_type = schema(agg_new).dataType.toString()
      //新增列名放到post api的字典中
      postdata(agg_new) = Map[String,String]("name" -> agg_new,"alias" -> agg_new,"type"->schema_type)
      }
    )

    df_list(temp_name) = tmptable


    df_list(temp_name).printSchema()
    df_list(temp_name).show()

  }
}
