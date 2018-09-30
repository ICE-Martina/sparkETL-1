package bi.spark.etl.alludf.datefun

import java.text.SimpleDateFormat
import java.util.Calendar

object dateReckon {
  //日期推算，spark提供的add_month计算有bug，2018-06-30减1个月，返回2018-05-31，不能返回2018-05-30
  val dateReckonMONTH:(String,Int)=>String=(date:String,diff:Int)=>{
    //传入日期为XXXX-XX-XX字符串
    if(date.length >= 10){
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val dt = dateFormat.parse(date)
      val rightNow = Calendar.getInstance()
      rightNow.setTime(dt)
      rightNow.add(Calendar.MONTH, diff)
      dateFormat.format(rightNow.getTime)
    }else{
      val dateFormat = new SimpleDateFormat("yyyy-MM")
      val dt = dateFormat.parse(date)
      val rightNow = Calendar.getInstance()
      rightNow.setTime(dt)
      rightNow.add(Calendar.MONTH, diff)
      dateFormat.format(rightNow.getTime)
    }
  }
}
