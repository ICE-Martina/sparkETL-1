//package bi.spark.etl
//
//import bi.spark.etl.aggregatepg.UdfObject
//import bi.spark.etl.joinpg.join._
//import bi.spark.etl.outputpg.output._
//import bi.spark.etl.cleanpg.clean._
//import bi.spark.etl.unionpg.union._
//import bi.spark.etl.pivotpg.pivot._
//import bi.spark.etl.aggregatepg.aggregate._
//import bi.spark.etl.inputpg.input._
//import org.apache.spark.sql.SparkSession
//
///**
//  * Description:
//  * Author: tangli
//  * Version: 1.0
//  * Create Date Time: 2018/7/17 0017 下午 2:32.
//  * Update Date Time:
//  * see
//  */
//
//object action {
//
//  /*所有操作在这个类里面做判断和执行，实例这个类时传入2个参数，
//    spark上下文和一个空的字典，字典存放dataframe的名字和数据*/
//  class DataProcess(sparksql:SparkSession,df_list:scala.collection.mutable.Map[String,org.apache.spark.sql.DataFrame]) {
//
//    //实例化AllUdf这个类，这个类可以添加任何需要用的udf函数
////    val alludf = new UdfClass(sparksql:SparkSession)
//    //调用functions方法，注册所有函数
//    UdfObject.functions(sparksql)
//
//    // 判断每一段json所需要的操作，并执行指定函数
//    def act(metajson: Map[String, Any]) = {
//
//      //获取处理类型
//      val act_type = metajson("type").toString
//
//      act_type match {
//
//        //读入操作(operation: input)
//        case "input" => {
//
//          input(sparksql,df_list,metajson)
//
//        }
//
//        //清洗操作(operation: clean)
//        case "clean" => {
//
//          clean(sparksql,df_list,metajson)
//
//        }
//
//        //聚合操作(operation: aggregate)
//        case "aggregate" => {
//
//          aggregate(sparksql,df_list,metajson)
//
//        }
//
//        //转置操作(operation: pivot)
//        case "pivot" => {
//
//          pivot(sparksql,df_list,metajson)
//
//        }
//
//        //关联操作(operation: join)
//        case "join" => {
//
//          join(sparksql,df_list,metajson)
//
//        }
//
//        //合并操作(operation: union)
//        case "union" => {
//
//          union(sparksql,df_list,metajson)
//
//        }
//
//        //输出操作(operation: output)
//        case "output" => {
//
//          output(sparksql,df_list,metajson)
//
//        }
//      }
//    }
//  }
//}
