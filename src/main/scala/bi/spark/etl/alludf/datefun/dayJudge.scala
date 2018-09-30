package bi.spark.etl.alludf.datefun

import java.text.SimpleDateFormat

object dayJudge {
  //日期判断函数，往前推得日期，day相等的就是推算有那天的日期，若不相等，证明推算后没那个日期
  //2018-07-31往前一个月，6月只有30日，所以匹配是，7-31返回空值
  val dayJudgeDay : (String,String) => String = (origin:String,last:String) => {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val datex = dateFormat.parse(origin).getDate
    //      val exoriginday = dateFormat.format(datex)
    val datey = dateFormat.parse(last).getDate
    //      val lastday = dateFormat.format(datey)
    if(datex==datey){
      last
    }else{
      //日不同，返回1970-01-01,不会匹配任何值
      "1970-01-01"
    }
  }
}
