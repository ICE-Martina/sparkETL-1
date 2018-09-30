package bi.spark.etl.unionpg

import bi.spark.etl.jsonAnalyse
import org.apache.spark.sql.SparkSession

/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/7/19 0019 下午 3:34.
  * Update Date Time:
  * see  还没实现，需要虾藻/艾绒给出json demo
  */

object union {
  def union(sparksql:SparkSession,df_list:scala.collection.mutable.Map[String,org.apache.spark.sql.DataFrame],metajson: Map[String, Any],postdata:scala.collection.mutable.Map[String,Map[String,String]])={

    //获取position，并转为key:value[Any]
    val position = jsonAnalyse.anyToDictAny(metajson("position"))

    //获取operation，并转为key:value[Any]
    val operation = jsonAnalyse.anyToDictAny(metajson("operation"))

    //获取operation的parms，并转为key:value[Any]
    val operation_parms = jsonAnalyse.anyToDictAny(operation("parms"))

    //获取temp_name
    val temp_name = metajson("temp_name").toString

    //获取处理类型
    val operation_type = operation("type").toString



  }
}
