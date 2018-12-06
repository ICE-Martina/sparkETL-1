package bi.spark.etl.pivotpg

import bi.spark.etl.jsonAnalyse
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, SparkSession}

import scala.collection.mutable.MutableList

/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/7/19 0019 下午 3:34.
  * Update Date Time:
  * see 实现转置功能（针对某一列转置）
  */

object pivot {
  def pivot(sparksql:SparkSession,df_list:scala.collection.mutable.Map[String,org.apache.spark.sql.DataFrame],metajson: Map[String, Any],postdata:scala.collection.mutable.Map[String,Map[String,String]])={

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
    val grouped_fields = operation_parms("grouped_fields").asInstanceOf[List[String]]

    //gourpBy所需要的列名
    var groupfield = MutableList[Column]()
    //把需要gourpBy的列整为长参数
    grouped_fields.foreach( c => groupfield += col(c) )

    //获取pivot_fields需要转置的列
    val pivot_fields = operation_parms("pivot_fields").toString
    //转置时所需要计算的列
    val agg_fields = operation_parms("agg_fields").asInstanceOf[List[Map[String,String]]]
    //转置时，数值的计算方式例如sum,count,max,min
    val agg_func = agg_fields.map( x => (x("agg_value"),x("agg_func")))


    df_list(source).show()
    var tmptable = df_list(source)
      .groupBy(groupfield:_*)
      .pivot(pivot_fields)
      .agg(agg_func.head,agg_func.tail:_*)
//      .agg(("pc_shop_flow_source__uv","sum"),("pc_shop_flow_source__alipay_trade_amt","sum"))

    /*
    修改列名
     */
    //旧列名
    val old_columns = tmptable.columns
    val schema = tmptable.schema
    //需要修改的列名
    val rename_columns = old_columns.diff(grouped_fields)
    //新列名，按照pivot_x来命名
    var i = 1
    rename_columns.foreach( n => {
      val schema_type = schema(n).dataType.toString()
      tmptable = tmptable.withColumnRenamed(n,s"pivot_${i}")
      /*withColumn(n,col(n).cast(DoubleType))*/
      //新增列名放到post api的字典中
      postdata(n) = Map[String,String]("name" -> s"pivot_${i}","alias" -> n,"type"->schema_type)
      i += 1
    })

    df_list(temp_name) = tmptable

    tmptable.show()

  }
}
