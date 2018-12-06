package bi.spark.etl.alludf.easy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.joda.time.format.DateTimeFormat

/*
把yyyy-MM-dd格式化为    yyyy年第X周(yyyy-MM-dd~yyyy-MM-dd)    周的开始日期和结束日期
 */

class YEAR_WEEK_RANGE (sparksql:SparkSession){

  val udfFunction: (String,Int) => String = (date: String,start: Int) => {
    val dtf = DateTimeFormat.forPattern("yyyy-MM-dd")
    /*  一:start为周几开始算为一周的开始，如果是1，即周一为一周的开始
        4即周四是这周的开始
        二:minusDays(start-1)，如果是，非周一为一周的开始，需要往前推start-1天
        推算后的日期会变成start-1的日期。
        如：date=2018-01-04，start=4时，minusDays(start-1)为前推3天，推算日期会变成2018-01-01
    */
    val dt = dtf.parseDateTime(date).minusDays(start-1)
    //获取推算后，当天未这周的第几天
    val dayOfweek = dt.dayOfWeek().get()
    //获取年份
    val year = dt.year().getAsString
    //获取第几周
    val weekOfyear = dt.weekOfWeekyear().get()
    /*前推日期之后(上述例子中2018-01-04，会视为2018-01-01)，计算出所在周的开始日期，结束日期*/
    //2018-01-01减一天再加4天，这周的开始日期为2018-01-04
    val weekFirstDay = dtf.print(dt.minusDays(dayOfweek).plusDays(start))
    //2018-01-01减一天再加4+6天，这周的结束日期为2018-01-10
    val weekLastDay = dtf.print(dt.minusDays(dayOfweek).plusDays(start+6))
    s"${year}年第${"%02d".format(weekOfyear)}周(${weekFirstDay}~${weekLastDay})"
  }


  //注册这个函数
  val fun_name = this.getClass.getName.split("\\.").last

  sparksql.udf.register(fun_name,udf(udfFunction))
}
