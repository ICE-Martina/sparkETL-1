package bi.spark.etl.alludf.easy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.joda.time.format.DateTimeFormat

/*
获取yyyy-MM-dd中的年yyyy int
 */

class YEAR (sparksql:SparkSession){

  val udfFunction: String => Int = (date : String) =>{
    val dtf = DateTimeFormat.forPattern("yyyy-MM-dd")
    val dt = dtf.parseDateTime(date)
    dt.year().get()
  }

  //注册这个函数
  val fun_name = this.getClass.getName.split("\\.").last

  sparksql.udf.register(fun_name,udf(udfFunction))
}
