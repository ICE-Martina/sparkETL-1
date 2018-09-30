package bi.spark.etl.outputpg

import java.sql.DriverManager

import bi.spark.etl.postpg.typeTrans.typeChang
import org.apache.spark.sql

object tableJudge {
  def createTable(df:sql.DataFrame,m_id:String):Unit={
    //表名
    val table_name = "t_" + m_id
    //查看是否有表
    val driver="org.apache.hive.jdbc.HiveDriver"
    Class.forName(driver)
    val (url,username,userpasswd)=("jdbc:hive2://node1:10000","","")
    val connection=DriverManager.getConnection(url,username,userpasswd)
    connection.prepareStatement("use default").execute()
    val sql="show tables"
    val statement=connection.prepareStatement(sql)
    val rs=statement.executeQuery()
//    val colnums = rs.getMetaData().getColumnCount
    val exists_table = scala.collection.mutable.Set[String]()
    while(rs.next()){
      exists_table.add(rs.getString(2))
      println(rs.getString(2))
    }

    if(!exists_table.contains(table_name)){
      val schema_list = df.schema
      var table_cols = ""
      for(i <- schema_list){
        println(i.name,typeChang(i.dataType.toString))
        table_cols += i.name + " " + typeChang(i.dataType.toString) + ","
      }
      val create_sql = s"""|create external table ${table_name}
                |(${table_cols.init})
                |row format delimited fields terminated by ','
                |location '/BIresult/${m_id}'
    """.stripMargin

      val create_res = connection.prepareStatement(create_sql).executeQuery()
      println(create_res)
    }
  }
}
