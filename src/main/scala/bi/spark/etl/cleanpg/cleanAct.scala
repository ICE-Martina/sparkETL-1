package bi.spark.etl.cleanpg

import org.apache.spark.sql.SparkSession
import bi.spark.etl.jsonAnalyse
/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/8/1 0001 上午 10:21.
  * Update Date Time:
  * see 解析clean内层操作，包括操作有  "filter"(过滤),"rename"(重命名),
  *                                    "calculate"(创建计算字段),"remove"(删除),
  *                                    "split"(分割)
  */

object cleanAct {
  def cleanType(sparksql:SparkSession,df_list:scala.collection.mutable.Map[String,org.apache.spark.sql.DataFrame],tmp_table:String,typeparms:Map[String,String],postdata:scala.collection.mutable.Map[String,Map[String,String]])={
    val clean_process = typeparms("clean_process").asInstanceOf[Map[String,Any]]
    //获取clean_process下，type的值
    val ctype = clean_process("type").toString

    //获取clean_process下，parms的值
    val parms:Map[String,String] = jsonAnalyse.anyToDictStr(clean_process("parms"))

    //每次小操作的临时表名，暂时没用
    val temp_name =typeparms("temp_name")


//    println(parms)

    ctype match {
        //筛选
      case "filter" => {
        //传入spark上下文，dataframe字典，操作数据源名字，结果表名字，每一个处理操作
        typeFilter.filter(sparksql,df_list,tmp_table,parms,postdata)
      }
      case "rename" => {
        //传入spark上下文，dataframe字典，操作数据源名字，结果表名字，每一个处理操作
        typeRename.rename(sparksql,df_list,tmp_table,parms,postdata)
      }
      case "calculate" => {
        //传入spark上下文，dataframe字典，操作数据源名字，结果表名字，每一个处理操作
        typeCalculate.calculate(sparksql,df_list,tmp_table,parms,postdata)
      }
      case "remove" => {
        //传入spark上下文，dataframe字典，操作数据源名字，结果表名字，每一个处理操作
        typeRemove.remove(sparksql,df_list,tmp_table,parms,postdata)
      }
      case "split" => {
        //还没实现此方法
        //传入spark上下文，dataframe字典，操作数据源名字，结果表名字，每一个处理操作
        typeSplit.split(sparksql,df_list,tmp_table,parms,postdata)
      }
      case "nullfill" => {
        //还没实现此方法
        //传入spark上下文，dataframe字典，操作数据源名字，结果表名字，每一个处理操作
        typeNullFill.nullfill(sparksql,df_list,tmp_table,parms,postdata)
      }
    }
  }
}
