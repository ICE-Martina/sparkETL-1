package bi.spark.etl.alludf.easy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.joda.time.format.DateTimeFormat

/*
获取yyyy-MM-dd中的年yyyyMM int
 */

class YEAR_MONTH (sparksql:SparkSession){

  val udfFunction: String => Int = (date: String) =>{
    val dtf = DateTimeFormat.forPattern("yyyy-MM-dd")
    val dtf2 = DateTimeFormat.forPattern("yyyyMM")
    val dt = dtf.parseDateTime(date)
    val ym = dtf2.print(dt)
    ym.toInt
  }

  //注册这个函数
  val fun_name = this.getClass.getName.split("\\.").last

  sparksql.udf.register(fun_name,udf(udfFunction))
}
