package bi.spark.etl.alludf.easy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/8/13 0013 下午 2:10.
  * Update Date Time:
  * see  一个类是一个函数，类名称规定全用大写
  */

class UDFDEMO(sparksql:SparkSession){




  val udfFunction = (/* 需要的参数 */) => /*返回值类型*/ {

    /*
    这个功能的主体函数
     */
    //返回值
    ""

  }




  //注册这个函数
  val fun_name = this.getClass.getName.split("\\.").last

  sparksql.udf.register(fun_name,udf(udfFunction))
}
