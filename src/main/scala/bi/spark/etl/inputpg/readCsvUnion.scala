package bi.spark.etl.inputpg


import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object readCsvUnion {
  //把读取csv的方法通用出来，直接传一个路径就能读取文件成为dtaaframe
  def re_csv (sparksql:SparkSession,path:String,header:Boolean,encoding:String):sql.DataFrame= {
    sparksql.read.format("csv")
      .option("header", header)
      .option("inferSchema", "true")
      .option("sep", ",")
      .option("encoding", encoding)
      .load(path)
  }
  // 递归读入的文件目录
  def readAndUnion(sparksql:SparkSession, filelist:List[String], header:Boolean, encoding:String):sql.DataFrame={
    //判断文件列表长度
    if(filelist.length <= 1){
      //只有一个文件直接读
      re_csv(sparksql:SparkSession,filelist.head,header:Boolean,encoding:String)
    }else{
      //多个文件，递归读入并拼接
      re_csv(sparksql:SparkSession,filelist.head,header:Boolean,encoding:String).union(readAndUnion(sparksql:SparkSession,filelist.tail,header:Boolean,encoding:String))
    }
  }
}
