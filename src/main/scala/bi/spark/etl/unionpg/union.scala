package bi.spark.etl.unionpg

import bi.spark.etl.cleanpg.cleanAct.cleanType
import bi.spark.etl.jsonAnalyse
import org.apache.spark.sql.SparkSession

/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/7/19 0019 下午 3:34.
  * Update Date Time:
  * see
  */

object union {
  def union(sparksql:SparkSession,df_list:scala.collection.mutable.Map[String,org.apache.spark.sql.DataFrame],metajson: Map[String, Any],postdata:scala.collection.mutable.Map[String,Map[String,String]])={

    //获取position，并转为key:value[Any]
    val position = jsonAnalyse.anyToDictAny(metajson("position"))

    //获取operation，并转为key:value[Any]
    val operation = jsonAnalyse.anyToDictAny(metajson("operation"))

    //获取operation的parms，并转为key:value[Any]
    val operation_parms = operation("parms").asInstanceOf[Map[String,Any]]

    //数据源
    val source_0 = metajson("source").asInstanceOf[List[String]](0)
    val source_1 = metajson("source").asInstanceOf[List[String]](1)

    //获取最外层temp_name
    val temp_name = metajson("temp_name").toString
    //复制需要操作的元数据成最终输出表，在clean的时候可对其重复操作
    println(source_0)
    df_list(source_0).show()
    println(source_1)
    df_list(source_1).show()
    df_list(temp_name) = df_list(source_0).union(df_list(source_1))

    df_list(temp_name).printSchema()
    //获取处理类型
    //    val operation_type = operation("type").toString




  }
}
