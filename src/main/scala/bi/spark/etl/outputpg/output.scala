package bi.spark.etl.outputpg

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.functions._
import bi.spark.etl.jsonAnalyse
import bi.spark.etl.outputpg.hadoopFile._
import org.apache.spark.sql.{SaveMode, SparkSession}
import bi.spark.etl.outputpg.tableJudge.createTable

/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/7/19 0019 上午 9:56.
  * Update Date Time:
  * see
  */

object output {

  def output(sparksql:SparkSession,df_list:scala.collection.mutable.Map[String,org.apache.spark.sql.DataFrame],metajson: Map[String, Any],postdata:scala.collection.mutable.Map[String,Map[String,String]])={
//    val dayJudge : (Any,Long) => Long = (origin:Any,origin2:Long) => {
//      origin2
//    }
//    val dayjudge = udf(dayJudge)
    //获取project_id
    val project_id = metajson("project_id").toString.toDouble.formatted("%.0f")
    //获取model_id
    val model_id = metajson("model_id").toString.toDouble.formatted("%.0f")
    val p_id = project_id.toString.toDouble.formatted("%.0f")
    val m_id = model_id.toString.toDouble.formatted("%.0f")
    //获取position，并转为key:value[Any]
    val position = jsonAnalyse.anyToDictAny(metajson("position"))

    //获取operation，并转为key:value[Any]
    val operation = jsonAnalyse.anyToDictAny(metajson("operation"))

    //获取operation的parms，并转为key:value[Any]
    val operation_parms = jsonAnalyse.anyToDictAny(operation("parms"))
    //parms转为字典
    val parms = jsonAnalyse.anyToDictAny(operation_parms("parms"))
    //输出路径和文件名
      //    /shop_space/53/result/10001/
    val output_name = s"/shop_space/${project_id}/result/${model_id}/"
    //输出文件格式，一期以csv文件输出
    val file_type = parms("file_type").toString
    //获取temp_name
    val temp_name = metajson("source").asInstanceOf[List[String]](0).toString
    //用overwrite方式写入
    //新增一列为project_id
    df_list(temp_name) = df_list(temp_name).withColumn("project_id",lit(project_id).cast("Long"))
    //新增列project_id放到post api的字典中
    postdata("project_id") = Map[String,String]("name" -> "project_id","alias" -> "project_id","type" -> "long")
    //不带表头写入
    df_list(temp_name).repartition(1)
      .write.mode("overwrite")
      .format(file_type)
      .option("encoding","utf8")
      .option("header",false)
      .save(output_name)

      //同步hdfs上的数据到指定目录给sql查询
    syncHadoopFile(output_name,p_id,m_id)
      //创建hive上的表
    createTable(df_list(temp_name),m_id)

    df_list(temp_name).show()
    df_list(temp_name).printSchema()
    df_list(temp_name)

  }
}
