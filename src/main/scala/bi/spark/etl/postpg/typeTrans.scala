package bi.spark.etl.postpg

/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/8/10 0010 下午 4:44.
  * Update Date Time:
  * see
  */

object typeTrans {
    def typeChange(str:String):String={
      //schema获取的类型都带有Type字样，传给api需要先整理
      str.replace("Type","").toLowerCase
    }

    def type2Hive(str:String):String={
      //schema获取的类型都带有Type字样，并转为hive能识别的类型
      val new_str = str.replace("Type","").toLowerCase
//      if (new_str == "long" || new_str == "integer"){"int"}
//      else {new_str}
      //多种不同类型的转换
      new_str match {
        case "long" => {"int"}
        case "integer" => {"int"}
        case _ => {new_str}
      }
    }
}
