package bi.spark.etl.inputpg
import java.io.{File, FileNotFoundException}
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}


object fileExists {
  def fileExistsJudge(filelist:scala.collection.mutable.Set[String]):scala.collection.mutable.Set[String]={
    //输出的set
    val out_set = scala.collection.mutable.Set[String]()
    //HDFS路径判断
    val configuration = new Configuration()
    val uri = URI.create("hdfs://ns1/")
    val fileSystem = FileSystem.get(uri,configuration)
    for(i <- filelist){
      try{
        val path = new Path(i.replace("*",""))
        //判断目录是否存在
        val file_list = FileUtil.stat2Paths(fileSystem.listStatus(path))
        //不存在的路径或者路径下没文件的目录删除
        if(fileSystem.exists(path) && file_list.nonEmpty){
          out_set.add(i)
        }
      }catch {
        case ex:FileNotFoundException => println(ex)
      }
    }
    ////    val uri1 = URI.create("hdfs://ns1/")
    ////    //获取文件系统
    ////    val fileSystem1 = FileSystem.get(uri1, configuration)
    ////    val path1 = new Path("/user/input/test")
    ////    val result = fileSystem1.exists(path1)
    ////    val path2 = new Path("/bi/model")
    ////    val result2 = fileSystem.exists(path2)
    //===========================================
    //本地文件判断
//        for(i <- filelist){
//          val file = new File(i)
//          //保留存在的路径且文件夹不为空
//          if(file.exists() && file.listFiles().nonEmpty){
//            out_set.add(i)
//          }
//        }
    out_set
//    filelist
  }
}
