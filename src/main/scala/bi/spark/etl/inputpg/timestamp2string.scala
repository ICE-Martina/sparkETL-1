package bi.spark.etl.inputpg

import org.apache.spark.sql
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._

object timestamp2String {
  //将读入时时TimeStamp的日期格式全转为String   "yyyy-MM-dd"格式
  def timeToString(df:sql.DataFrame,coltype:String):sql.DataFrame={
    df.withColumn(coltype,to_date(df(coltype),"yyyy-MM-dd").cast(StringType))
  }
}
