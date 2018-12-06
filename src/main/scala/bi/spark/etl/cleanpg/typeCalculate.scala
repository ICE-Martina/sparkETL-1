package bi.spark.etl.cleanpg


import bi.spark.etl.registerudf.ClassUtil
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer


/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/8/1 0001 下午 1:54.
  * Update Date Time:
  * see
  */

object typeCalculate {

    def calculate(sparksql:SparkSession,df_list:scala.collection.mutable.Map[String,org.apache.spark.sql.DataFrame],tmp_table:String,parms:Map[String,String],postdata:scala.collection.mutable.Map[String,Map[String,String]])={
//      testudf.test(sparksql:SparkSession)
//      df_list(tmp_table).createOrReplaceTempView("tmptable")
      // 每个最细粒度的操作
      val addfield_expr:String = parms("addfield_expr")
      //新增列的英文名字
      val addfield_dim:String = parms("addfield_dim")
      //返回给api对应的中文名
      val addfield_name:String = parms("addfield_name")

//===================================反射类名，判断是否有复杂列新增
      //列出complex所有类名
      val udfclass = ClassUtil.getClasses("bi.spark.etl.alludf.complex").toArray
      //用于注册的类名称列表
      val udflist = ArrayBuffer[String]()
      //遍历所有类的名字   例：class bi.spark.etl.alludf.UdfDemo
      udfclass.foreach( i =>{
          //分割，取后面的符
            udflist += i.toString.split(" ").last
      })
      //筛选不包含$的元素，打jar包后，有些类会生成xxx$xx字样
      val udfnames = udflist.filterNot( _.contains("$"))
      //当前calculate的函数名字
      val funcname = s"bi.spark.etl.alludf.complex.${addfield_expr.split("\\(")(0)}"
      //判断是否有复杂列生成，有的话，执行true的内容，没的话，执行简答新增列
      if(udfnames.contains(funcname)){
      //存在复杂新增情况
//      println(addfield_expr)
      val classA = Class.forName(funcname)
      val method = classA.getDeclaredMethod("func",classOf[SparkSession],classOf[sql.DataFrame],classOf[Map[String,String]])
      val tmptable:sql.DataFrame = method.invoke(classA.newInstance(),sparksql,df_list(tmp_table),parms).asInstanceOf[sql.DataFrame]
//      method.invoke(newInst, "5")//.asInstanceOf[sql.DataFrame]

//      val tmptable:sql.DataFrame  = df_list(tmp_table).selectExpr("*")

      df_list(tmp_table) = tmptable

      val oldname:String = tmptable.columns.last
            println(oldname,addfield_dim)

      df_list(tmp_table) = tmptable.withColumnRenamed(oldname,addfield_dim)
      //获取新增列的schema
      val schema = df_list(tmp_table).schema
      val schema_type = schema(addfield_dim).dataType.toString

      //新增列名放到post api的字典中
      postdata(addfield_dim) = Map[String,String]("name" -> addfield_dim,"alias" -> addfield_name,"type" -> schema_type)


      }else{
       //不存在复杂新增情况

      val tmptable:sql.DataFrame  = df_list(tmp_table).selectExpr("*",addfield_expr)

      val oldname:String = tmptable.columns.last

      //修改新增列列名
      df_list(tmp_table) = tmptable.withColumnRenamed(oldname,addfield_dim)
      //获取新增列的schema
      val schema = df_list(tmp_table).schema
      val schema_type = schema(addfield_dim).dataType.toString

      //新增列名放到post api的字典中
      postdata(addfield_dim) = Map[String,String]("name" -> addfield_dim,"alias" -> addfield_name,"type" -> schema_type)

//      df_list(tmp_table)
      }

//=================================================================================================
      //用selectExpr进行字段创建操作,这种操作有点是重新查表，然后外加计算字段,testudf(c3)
//      val tmptable:sql.DataFrame  = df_list(tmp_table).selectExpr("*",addfield_expr)
      //用withColumn进行字段创建操作，这种方法灵活性不够，testudf(c3)
//      val tmptable:sql.DataFrame  = df_list(tmp_table).withColumn("newname",testudf(df_list(tmp_table)("c3")))

      //用select进行字段创建操作,这种操作有点是重新查表，注册临时表，然后外加计算字段,testudf(c3)
//      val tmptable:sql.DataFrame  = sparksql.sql(s"select *,${addfield_expr} from tmptable")



//      val oldname:String = tmptable.columns.last
//
//      //修改新增列列名
//      df_list(tmp_table) = tmptable.withColumnRenamed(oldname,addfield_dim)
//      //获取新增列的schema
//      val schema = df_list(tmp_table).schema
//      val schema_type = schema(addfield_dim).dataType.toString
//
//      //新增列名放到post api的字典中
//      postdata(addfield_dim) = Map[String,String]("name" -> addfield_dim,"alias" -> addfield_name,"type" -> schema_type)
//
      df_list(tmp_table)
    }
}
