package bi.spark.etl.inputpg
import scala.collection.mutable.MutableList
/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/8/9 0009 下午 4:48.
  * Update Date Time:
  * see
  */
//interaction__thedate 转换为 thedate

object selectedDim {
  def reselectdim(dimlist:List[String]):List[String]={
    var newlist:List[String] = List[String]()
    for(i <- dimlist){
      newlist = newlist :+ i.split("__")(1)
    }
    newlist
  }
}
