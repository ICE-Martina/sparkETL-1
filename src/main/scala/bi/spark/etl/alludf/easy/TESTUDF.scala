package bi.spark.etl.alludf.easy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/8/1 0001 下午 6:52.
  * Update Date Time:
  * see
  */

class TESTUDF(sparksql:SparkSession){
    //用于测试的自定义函数，输入一列，转为100
//  println(this.getClass.getName)

    val udfFunction : Double => String = (x: Double) => {
      if(x <=1.5){ "a"}
      else if(x<=2){"c"}
      else{ "b" }
    }
    //注册这个函数
  val fun_name = this.getClass.getName.split("\\.").last
//  println(fun_name)
  sparksql.udf.register(fun_name,udf(udfFunction))
}
