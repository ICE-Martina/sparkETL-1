package bi.spark.etl.cleanpg

import bi.spark.etl.jsonAnalyse
import org.apache.spark.sql.SparkSession
import bi.spark.etl.cleanpg.cleanAct.cleanType

/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/7/19 0019 下午 3:30.
  * Update Date Time:
  * see  这里解释最外层json的clean动作，并循环operation_parms的内容，往下调用
  */

object clean {
  def clean(sparksql:SparkSession,df_list:scala.collection.mutable.Map[String,org.apache.spark.sql.DataFrame],metajson: Map[String, Any],postdata:scala.collection.mutable.Map[String,Map[String,String]])={

    //获取position，并转为key:value[Any]
    val position = jsonAnalyse.anyToDictAny(metajson("position"))

    //获取operation，并转为key:value[Any]
    val operation = jsonAnalyse.anyToDictAny(metajson("operation"))

    //获取operation的parms，并转为key:value[Any]
    val operation_parms = operation("parms").asInstanceOf[List[Map[String,String]]]

    //数据源
    val source = metajson("source").asInstanceOf[List[String]](0)

    //获取最外层temp_name
    val temp_name = metajson("temp_name").toString
    //复制需要操作的元数据成最终输出表，在clean的时候可对其重复操作
    df_list(temp_name) = df_list(source)

    df_list(temp_name).printSchema()
    //获取处理类型
//    val operation_type = operation("type").toString

    operation_parms.foreach( parm => {
      //传入spark上下文，dataframe字典，操作数据源名字，结果表名字，每一个处理操作参数
      cleanType(sparksql,df_list,temp_name,parm,postdata).show()
    })

  }
}
