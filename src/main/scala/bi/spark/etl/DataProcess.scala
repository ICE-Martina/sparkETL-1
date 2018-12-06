package bi.spark.etl

import bi.spark.etl.aggregatepg.aggregate.aggregate
import bi.spark.etl.cleanpg.clean.clean
import bi.spark.etl.inputpg.input.input
import bi.spark.etl.joinpg.join.join
import bi.spark.etl.outputpg.output.output
import bi.spark.etl.pivotpg.pivot.pivot
import bi.spark.etl.registerudf.udfObject
import bi.spark.etl.unionpg.union.union
import org.apache.spark.sql.SparkSession

/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/8/9 0009 下午 6:34.
  * Update Date Time:
  * see
  */
/*所有操作在这个类里面做判断和执行，实例这个类时传入2个参数，
  spark上下文和一个空的字典，字典存放dataframe的名字和数据*/
class DataProcess(sparksql:SparkSession,df_list:scala.collection.mutable.Map[String,org.apache.spark.sql.DataFrame]) {

  //实例化AllUdf这个类，这个类可以添加任何需要用的udf函数
  //    val alludf = new UdfClass(sparksql:SparkSession)
  //调用functions方法，注册所有alludf.easy下的函数
  udfObject.functions(sparksql)

  // 判断每一段json所需要的操作，并执行指定函数
  def actions(metajson: Map[String, Any], postdata:scala.collection.mutable.Map[String,Map[String,String]]) = {

    //获取处理类型
    val act_type = metajson("type").toString
    println(act_type)
    println(metajson("temp_name").toString)
    act_type match {

      //读入操作(operation: input)
      case "input" => {

        input(sparksql,df_list,metajson,postdata)

      }

      //清洗操作(operation: clean)
      case "clean" => {

        clean(sparksql,df_list,metajson,postdata)

      }

      //聚合操作(operation: aggregate)
      case "aggregate" => {

        aggregate(sparksql,df_list,metajson,postdata)

      }

      //转置操作(operation: pivot)
      case "pivot" => {

        pivot(sparksql,df_list,metajson,postdata)

      }

      //关联操作(operation: join)
      case "join" => {

        join(sparksql,df_list,metajson,postdata)

      }

      //合并操作(operation: union)
      case "union" => {

        union(sparksql,df_list,metajson,postdata)

      }

      //输出操作(operation: output)
      case "output" => {

        output(sparksql,df_list,metajson,postdata)

      }
    }
  }
}
