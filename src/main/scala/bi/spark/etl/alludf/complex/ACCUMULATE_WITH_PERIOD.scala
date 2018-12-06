package bi.spark.etl.alludf.complex

import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession


class ACCUMULATE_WITH_PERIOD{

  def func(sparksql:SparkSession,df:sql.DataFrame,parms:Map[String,String]):sql.DataFrame={
    //获取field下的内容
    val field = parms("field").asInstanceOf[Map[String,String]]
    val re_pattern = """\w+""".r
    //ACCUMULATE_WITH_PERIOD('MONTH', 1, 'statsellplan__thedate','trade__alipay_trade_amt')
    //expr(0)是函数名字，expr(1)第一个参数：DAY/MONTH,expr(2)第二个参数日期列XXX__thedate,expr(3)第三个参数金额列
    val expr = (re_pattern findAllIn parms("addfield_expr")).toList
    expr.foreach( i => {println(i)})
    println("=======")
    //未完成的功能
//    df.withColumn("Accumulate",lit(0))
    if(expr(1) == "MONTH"){
      var df_tmp = df.withColumn("YearMonth",substring(col(expr(3)),0,7))
      val first_2_now_window = Window.partitionBy("YearMonth").orderBy(col(expr(3)))
      df_tmp = df_tmp.select(col("YearMonth"),col(expr(3)).as("TmpColumn"),sum(col(expr(4))).over(first_2_now_window).as("sum_duration")).drop("YearMonth")
      df_tmp = df.join(df_tmp, df(expr(3)) === df_tmp("TmpColumn"),"left").drop("TmpColumn")
      df_tmp
    }else if(expr(1) == "YEAR"){
      var df_tmp = df.withColumn("Year",substring(col(expr(3)),0,4))
      df_tmp.show()
      val first_2_now_window = Window.partitionBy("Year").orderBy(col(expr(3)))
      df_tmp = df_tmp.select(col("Year"),col(expr(3)).as("TmpColumn"),sum(col(expr(4))).over(first_2_now_window).as("sum_duration")).drop("Year")
      df_tmp.show()
      df_tmp = df.join(df_tmp, df(expr(3)) === df_tmp("TmpColumn"),"left").drop("TmpColumn")
      df_tmp
    }else{
      df.withColumn("Accumulate",lit(0))
    }

//    df.withColumn("Accumulate",lit(0))
  }
}
