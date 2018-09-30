package bi.spark.etl.cleanpg

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/8/2 0002 上午 11:46.
  * Update Date Time:
  * see
  */

object typeFilter {

    def filter(sparksql:SparkSession,df_list:scala.collection.mutable.Map[String,org.apache.spark.sql.DataFrame],tmp_table:String,parms:Map[String,String],postdata:scala.collection.mutable.Map[String,Map[String,String]])={

      // 每个最细粒度的操作
//      val selected_dim:String = parms("selected_dim")

      val filter_expr:String = parms("filter_expr")

      //filter方法，筛选符合条件的内容
      val tmptable:sql.DataFrame  = df_list(tmp_table).filter(filter_expr)


      df_list(tmp_table) = tmptable

      df_list(tmp_table)
    }
}
