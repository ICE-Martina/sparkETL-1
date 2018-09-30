package bi.spark.etl.alludf.complex

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession


class ACCUMULATE_WITH_PERIOD{

  def func(sparksql:SparkSession,df:sql.DataFrame,parms:Map[String,String]):sql.DataFrame={
    //获取field下的内容
    val field = parms("field").asInstanceOf[Map[String,String]]
    val re_pattern = """\w+""".r
    //Value_Last_Month("DAY",[thedate], [Profit])
    //expr(0)是函数名字，expr(1)第一个参数：DAY/MONTH,expr(2)第二个参数日期列XXX__thedate,expr(3)第三个参数金额列
    val expr = (re_pattern findAllIn parms("addfield_expr")).toList
    for(i <- expr){println(i)}
    println("=======")
    //未完成的功能
//    df.withColumn("Accumulate",lit(0))
    if(expr(1) == "MONTH"){
      var df_tmp = df.withColumn("YearMonth",substring(col(expr(3)),0,7))
      val first_2_now_window = Window.partitionBy("YearMonth").orderBy(col(expr(3)))
      df_tmp = df_tmp.select(col("YearMonth"),col(expr(3)).as("TmpColumn"),sum(col(expr(4))).over(first_2_now_window).as("sum_duration")).drop("YearMonth")
      df_tmp = df.join(df_tmp, df(expr(3)) === df_tmp("TmpColumn"),"left").drop("TmpColumn")
      df_tmp
    }else{
      df.withColumn("Accumulate",lit(0))
    }

//    df.withColumn("Accumulate",lit(0))
  }
}
