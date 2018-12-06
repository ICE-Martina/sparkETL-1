package bi.spark.etl.cleanpg

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object typeNullFill {
  def nullfill(sparksql:SparkSession,df_list:scala.collection.mutable.Map[String,org.apache.spark.sql.DataFrame],tmp_table:String,parms:Map[String,String],postdata:scala.collection.mutable.Map[String,Map[String,String]])={

    // 每个最细粒度的操作
    //      val selected_dim:String = parms("selected_dim")

    val null_field:String = parms("null_field")

    val fill_value = parms("fill_value").toLong

    val fillColValues = Map(null_field -> fill_value)

    //filter方法，筛选符合条件的内容
    val tmptable:sql.DataFrame  = df_list(tmp_table).na.fill(fillColValues)


    df_list(tmp_table) = tmptable

    df_list(tmp_table)
  }

}
