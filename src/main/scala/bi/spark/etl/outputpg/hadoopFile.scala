package bi.spark.etl.outputpg

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

//用以同步不同商店id，但相同类型model的结果表到一个目录，提供表指向文件目录的查询
object hadoopFile {
  def syncHadoopFile(file:String,p_id:String,m_id:String):Unit={
    val conf = new Configuration();// 加載配制文件
    val uri = new URI("hdfs://qmyrc/"); // 要连接的资源位置
    val fileSystem = FileSystem.get(uri,conf,"root")
    //目标文件路径
    val files = fileSystem.listStatus(new Path(file))
    //选取包含part字符的文件名
    val filename = files.filter( _.getPath.getName.contains("part"))
    //修改系统生成的文件名
    //获取旧文件名
    //  /shop_space/53/result/10001/part-00000-272ee45b-9454-49b6-8267-040769fe0b14-c000.csv
    val oldfile = file + filename(0).getPath.getName
    val oldpath = new Path(oldfile)
    //指定的文件名
    //  /shop_space/53/result/10001/  +   53 + _ + 10001 +  .csv   /shop_space/53/result/10001/53_10001.csv
    val newpath = new Path(file + p_id +"_" + m_id + ".csv")
    //修改名字
    fileSystem.rename(oldpath,newpath)
    //对应结果目录集
    //指定/BIresult/10001    跟model_id    文件名问53_10001.csv   project_id_model_id.csv
    val target_path = new Path(s"/BIresult/${m_id}")
//    FileUtil.stat2Paths(fileSystem.listStatus(target_path))
    if(!fileSystem.exists(target_path)){
      println(s"make dir /BIresult/${m_id}")
      fileSystem.mkdirs(target_path)
      FileUtil.copy(fileSystem, newpath, fileSystem, target_path, false, conf)
    }else{
      FileUtil.copy(fileSystem, newpath, fileSystem, target_path, false, conf)
    }
    //复制文件到指定目录下
//    println(oldfile)
  }
}
