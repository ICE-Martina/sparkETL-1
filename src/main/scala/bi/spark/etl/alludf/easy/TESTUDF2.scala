package bi.spark.etl.alludf.easy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/8/7 0007 上午 10:30.
  * Update Date Time:
  * see
  */

class TESTUDF2(sparksql:SparkSession){
//  println(this.getClass.getName)
//  println(s)
  val udfFunction : (Int,Int) => Int = (x:Int,y:Int) => {
    x * 2 + y * 2
  }
  val fun_name = this.getClass.getName.split("\\.").last
//  println(fun_name)
  sparksql.udf.register(fun_name,udf(udfFunction))
}
