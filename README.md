bi modeld
#20180810
基本清洗流程
1.读取hdfs数据
2.数据清洗
3.udf功能，预留自定义接口
4.计算后的字段映射返回给http://192.168.15.51:8181/docs/bi_facade/144这里描述的api
5.结果放到hdfs相应目录
#20180814
spark-submit --master spark://node1:7077 sparkETLv2.jar 101.json
最后的101.json在实际运行时，传入model_id，程序会在hdfs相应的目录下
  读取相应的json内容
#20180815
spark-submit --master spark://node1:7077 sparkETLv2.jar model_id
model_id为hdfs上/bi/model的json文件，只需填写model_id，代码会自动拼接为model_id.json
#20180820
优化了代码，registerudf为在网上引用了一段java的代码，可以解析jar包中一个package下的类名，这个用于自动注册udf时，可简化读取方式。