package bi.spark.etl.cleanpg


import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/8/2 0002 下午 3:25.
  * Update Date Time:
  * see
  */

object typeRename {
  def rename(sparksql:SparkSession,df_list:scala.collection.mutable.Map[String,org.apache.spark.sql.DataFrame],tmp_table:String,parms:Map[String,String],postdata:scala.collection.mutable.Map[String,Map[String,String]])={
    //      testudf.test(sparksql:SparkSession)
    //      df_list(tmp_table).createOrReplaceTempView("tmptable")
    // 每个最细粒度的操作
    val selected_dim:String = parms("selected_dim")

    val changed_name:String = parms("changed_name")

    //修改字段名
    val tmptable:sql.DataFrame  = df_list(tmp_table).withColumnRenamed(selected_dim,changed_name)


    df_list(tmp_table) = tmptable

    //没有新的json demo没修改post data api的字典
    //postdata() = Map[String,String]("name"->addfield_dim,"alias"->addfield_name,"type"->)

    df_list(tmp_table)
  }
}
