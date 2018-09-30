package bi.spark.etl.joinpg

import bi.spark.etl.jsonAnalyse
import org.apache.spark.sql.SparkSession

/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/7/18 0018 下午 6:04.
  * Update Date Time:
  * see  join操作
  */

object join {
  def join(sparksql:SparkSession,df_list:scala.collection.mutable.Map[String,org.apache.spark.sql.DataFrame],metajson: Map[String, Any],postdata:scala.collection.mutable.Map[String,Map[String,String]])={

    //获取position，并转为key:value[Any]
    val position = jsonAnalyse.anyToDictAny(metajson("position"))

    //获取operation，并转为key:value[Any]
    val operation = jsonAnalyse.anyToDictAny(metajson("operation"))

    //获取operation的parms，并转为key:value[Any]
    val operation_parms = jsonAnalyse.anyToDictAny(operation("parms"))

    //获取temp_name
    val temp_name = metajson("temp_name").toString

    //做join操作的两个表
    val tables = jsonAnalyse.anyToDictStr(operation_parms("join_tables"))

    val join_fields = operation_parms("join_fields_list").asInstanceOf[List[Map[String,String]]](0)
    df_list(tables("left_table")).show()
    df_list(tables("right_table")).show()
    //join的主函数
    df_list(temp_name) = df_list(tables("left_table")).join(df_list(tables("right_table")),df_list(tables("left_table"))(join_fields("left_field")) === df_list(tables("right_table"))(join_fields("right_field")),operation_parms("type").toString)

    df_list(temp_name).show()

  }
}
