package bi.spark.etl.cleanpg

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/8/2 0002 下午 2:00.
  * Update Date Time:
  * see
  */

object typeRemove {
  def remove(sparksql:SparkSession,df_list:scala.collection.mutable.Map[String,org.apache.spark.sql.DataFrame],tmp_table:String,parms:Map[String,String],postdata:scala.collection.mutable.Map[String,Map[String,String]])={

    // 选择要删除的列
    val selected_dim:String = parms("selected_dim")
    //是否删除，这个值没用上，因为直接判断列就做删除
    val is_remove:String = parms("is_remove")

    //删除字段
    val tmptable:sql.DataFrame  = df_list(tmp_table).drop(selected_dim)

    df_list(tmp_table) = tmptable

    //删除post给api时，指定的键值对
    postdata.remove(selected_dim)

    df_list(tmp_table)
  }
}
