package bi.spark.etl.registerudf

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/8/1 0001 下午 6:36.
  * Update Date Time:
  * see  用反射的方式，实例每个自定义函数，在alludf里面的类，需要重写方法，实例化是可以自动注册
  * 在udflist中添加上函数的名字
  *
  */

object udfObject {
  def functions(sparksql: SparkSession) = {
    //指定获取这个包下的类
    val udfclass = ClassUtil.getClasses("bi.spark.etl.alludf.easy").toArray
    //用于注册的类名称列表
    val udflist = ArrayBuffer[String]()
    //遍历所有类的名字   例：class bi.spark.etl.alludf.UdfDemo
    udfclass.foreach( i => {
      //分割，取后面的符
      udflist += i.toString.split(" ").last
    })
    //筛选不包含$的元素，打jar包后，有些类会生成xxx$xx字样
    val udfnames = udflist.filterNot( _.contains("$"))


    udfnames.foreach( i =>{
      //把每个类实例化，同时会注册成sparksql函数
      val classA = Class.forName(i)
      val cons = classA.getConstructors
      cons(0).newInstance(sparksql)
    })
  }
}



