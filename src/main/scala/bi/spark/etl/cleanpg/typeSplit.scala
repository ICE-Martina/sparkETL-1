package bi.spark.etl.cleanpg


import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/8/3 0003 上午 10:43.
  * Update Date Time:
  * see
  */

object typeSplit {
  def split(sparksql:SparkSession,df_list:scala.collection.mutable.Map[String,org.apache.spark.sql.DataFrame],tmp_table:String,parms:Map[String,String],postdata:scala.collection.mutable.Map[String,Map[String,String]])={
    //      testudf.test(sparksql:SparkSession)
    //      df_list(tmp_table).createOrReplaceTempView("tmptable")
    // 每个最细粒度的操作
    val selected_dim:String = parms("selected_dim")

    val changed_name:String = parms("changed_name")


    /*
    * 还没实现此方法
    * */
    //修改字段名
    val tmptable:sql.DataFrame  = df_list(tmp_table)


    df_list(tmp_table) = tmptable



    df_list(tmp_table)
  }
}
