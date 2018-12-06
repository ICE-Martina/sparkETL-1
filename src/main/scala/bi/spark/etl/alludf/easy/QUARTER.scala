package bi.spark.etl.alludf.easy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.joda.time.format.DateTimeFormat

class QUARTER(sparksql:SparkSession){

  val udfFunction: String => String = (date : String) =>{
    val dtf = DateTimeFormat.forPattern("yyyy-MM-dd")
    val dt = dtf.parseDateTime(date)
    val month = dt.monthOfYear().get()
    val year = dt.year().get()
    val quar = month match {
      case  _ if 1 <= month && month <= 3 =>  "一"
      case  _ if 4 <= month && month <= 6 =>  "二"
      case  _ if 7 <= month && month <= 9 =>  "三"
      case  _ if 10 <= month && month <= 12 => "四"
    }
    s"${year}年第${quar}季度"
  }


  //注册这个函数
  val fun_name = this.getClass.getName.split("\\.").last

  sparksql.udf.register(fun_name,udf(udfFunction))
}
