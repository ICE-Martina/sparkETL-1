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
    def typeChang(str:String):String={
      //schema获取的类型都带有Type字样，传给api需要先整理
      str.replace("Type","").toLowerCase
    }
}
