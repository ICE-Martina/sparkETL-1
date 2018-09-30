package bi.spark.etl.inputpg

import bi.spark.etl.jsonAnalyse
import org.apache.spark.sql.SparkSession
import bi.spark.etl.inputpg.selectedDim.reselectdim
import bi.spark.etl.inputpg.readCsvUnion.readAndUnion
import org.apache.spark._
import bi.spark.etl.postpg.transForm.dataTrans
import bi.spark.etl.inputpg.fileExists._
import bi.spark.etl.inputpg.timestamp2string.timeToString
import org.apache.spark.sql.types.TimestampType
/**
  * Description:
  * Author: tangli
  * Version: 1.0
  * Create Date Time: 2018/7/18 0018 下午 6:52.
  * Update Date Time:
  * see  读取数据
  */

object input {
  def input(sparksql:SparkSession,df_list:scala.collection.mutable.Map[String,sql.DataFrame],metajson: Map[String, Any],postdata:scala.collection.mutable.Map[String,Map[String,String]])={

    //获取position，并转为key:value[Any]
    val position = jsonAnalyse.anyToDictAny(metajson("position"))

    //获取operation，并转为key:value[Any]
    val operation = jsonAnalyse.anyToDictAny(metajson("operation"))

    //获取operation的parms，并转为key:value[Any]
    val operation_parms = jsonAnalyse.anyToDictAny(operation("parms"))

    //获取数据源的方式source,一期暂定2种形式，mysql和hdfs上的csv文件
    val source = jsonAnalyse.anyToDictAny(metajson("source"))

    val source_type = source("type").toString

    //获取temp_name
    val temp_name = metajson("temp_name").toString

    //解析parms的内容
    val text_settings = jsonAnalyse.anyToDictStr(operation_parms("text_settings"))

    //获取dim_settings内容
    val dim_settings = jsonAnalyse.anyToDictAny(operation_parms("dim_settings"))

    //用户选择的维度组装__后的数组
    val selected_dim_list = dim_settings("selected_dim_list").asInstanceOf[List[String]]

    //解析读入数据时，源数据的列名
    val metadim = reselectdim(selected_dim_list)

    //数据源表名称
    val table = metajson("table").toString

    source_type match {

        //mysql读入
        //0.1.2json未修改
      case "mysql" => {

        //解析json，获取operation下source的config字典
        val source_config = jsonAnalyse.anyToDictStr(jsonAnalyse.anyToDictAny(operation_parms("source"))("config"))
        //获取数据库ip:host，传入的形式是   ip:host
        val host_port = source_config("host")
        //获取库名
        val dbname = source_config("db")
        // 读入mysql的接口，把读入的表放到一个全局的dict中
        df_list(temp_name) = sparksql.read.format("jdbc")
          .option("url",s"jdbc:mysql://${host_port}/${dbname}?useSSL=false")
          .option("dbtable",table)
          .option("user",source_config("username"))
          .option("password",source_config("password"))
          .load()

        ////按用户提交的需求，筛选所需要的字段
        if(selected_dim_list.length >= 1 ){
          df_list(temp_name) = df_list(temp_name).selectExpr(selected_dim_list:_*)
        }else{
          df_list(temp_name)
        }
        df_list(temp_name)
      }

        //csv读入,保证单次读入的文件，字段格式一致
      case "excel" => {

        //解析json，获取operation下source的config字典
        val source_config = jsonAnalyse.anyToDictAny(source("config"))

        //文件的路径
        val file_path = source_config("path").asInstanceOf[List[String]]

        //默认读取文件有表头 true
        var header = true
        //传过来的header值不为first_contains，则为没表头，表头将按系统默认方式配置表头
        if(text_settings("header").toString != "contains"){
           header = false
        }
        //把读取csv的方法通用出来，直接传一个路径就能读取文件成为dtaaframe
//        def re_csv (path:String):sql.DataFrame= {
//            sparksql.read.format("csv")
//            .option("header", header)
//            .option("inferSchema", "true")
//            .option("sep", ",")
//            .option("encoding", text_settings("character_set"))
//            .load(path)
//        }
//        // 递归读入的文件目录
//        def reunion(filelist:List[String]):sql.DataFrame={
//          //判断文件列表长度
//          if(filelist.length <= 1){
//            //只有一个文件直接读
//              re_csv(filelist.head)
//          }else{
//            //多个文件，递归读入并拼接
//              re_csv(filelist.head).union(reunion(filelist.tail))
//          }
//        }
        //遍历文件路径为set类型
        var set_path = scala.collection.mutable.Set[String]()
        for(i <- file_path){
          //出去原路径描述的*
          set_path += i
        }
        //筛选不存在的路径
        val exists_set = fileExistsJudge(set_path)
        //转为list
        val exists_list = exists_set.toList
        //按用户提交的需求，筛选所需要的字段
        if(exists_list.nonEmpty ){
          df_list(temp_name) = readAndUnion(sparksql,exists_list,header,text_settings("character_set")).selectExpr(metadim:_*)
        }else{
          //传入值为空是，则返回所有列
          df_list(temp_name) = readAndUnion(sparksql,exists_list,header,text_settings("character_set"))
        }
        //读入数据的数据类型
        val dfschema =df_list(temp_name).schema
        //返回结果
        for(i <- Array.range(0,metadim.length)){
          if(dfschema(i).dataType == TimestampType){
            //时间类型的列转为字符串 XXXX-XX-XX的日期格式
            df_list(temp_name) = timeToString(df_list(temp_name),metadim(i))
          }
          df_list(temp_name) = df_list(temp_name).withColumnRenamed(metadim(i),selected_dim_list(i))
        }
        df_list(temp_name).printSchema()
        dataTrans(postdata,df_list(temp_name))

        df_list(temp_name).show()

      }
    }
  }
}
