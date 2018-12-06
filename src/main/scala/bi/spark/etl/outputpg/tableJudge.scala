package bi.spark.etl.outputpg

import java.sql.DriverManager

import bi.spark.etl.postpg.typeTrans.{typeChange,type2Hive}
import org.apache.spark.sql

object tableJudge {
  def createTable(df:sql.DataFrame,m_id:String):Unit={
    //表名
    val table_name = "t_" + m_id
    //查看是否有表
    val driver="org.apache.hive.jdbc.HiveDriver"
    Class.forName(driver)
    val (url,username,userpasswd)=("jdbc:hive2://bdmaster1:10000","","")
    val connection=DriverManager.getConnection(url,username,userpasswd)
    connection.prepareStatement("use default").execute()
    val sql="show tables"
    val statement=connection.prepareStatement(sql)
    val rs=statement.executeQuery()

    val exists_table = scala.collection.mutable.Set[String]()
    while(rs.next()){
      exists_table.add(rs.getString(1))
      println(rs.getString(1))
    }

    if(!exists_table.contains(table_name)){
      println(s"create table ${table_name}")
      val schema_list = df.schema
      var table_cols = ""
      for(i <- schema_list){
        println(i.name,typeChange(i.dataType.toString))
        table_cols += i.name + " " + type2Hive(i.dataType.toString) + ","
      }
      val create_sql = s"""|create external table ${table_name}
                |(${table_cols.init})
                |row format delimited fields terminated by ','
                |location '/BIresult/${m_id}'
    """.stripMargin

      val create_res = connection.prepareStatement(create_sql).execute()
      println(create_res)
    }else if(df.columns.contains("pivot_1")/*判断是否有转置*/){
      //转置过的表需要删了旧的表结构，再创建
      println(s"drop table ${table_name}")
      connection.prepareStatement(s"drop table ${table_name}").execute()
      println(s"create table ${table_name}")
      val schema_list = df.schema
      var table_cols = ""
      for(i <- schema_list){
        println(i.name,typeChange(i.dataType.toString))
        table_cols += i.name + " " + type2Hive(i.dataType.toString) + ","
      }
      val create_sql = s"""|create external table ${table_name}
                           |(${table_cols.init})
                           |row format delimited fields terminated by ','
                           |location '/BIresult/${m_id}'
    """.stripMargin

      val create_res = connection.prepareStatement(create_sql).execute()
      println(create_res)
    }
  }
}
