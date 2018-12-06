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
#20180921
1添加了VALUE_LAST_MONTH和VALUE_LAST_YEAR两个功能
2完善了input对路径的存在判断
3output时，判断hive是否已经有对应的查询表，如果没有就创建
4output时，把每个店的输出文件改名，并copy到指定临时表
#20181009
1之前版本通过spark-thriftserver查询是否存在t_10001等表，由于需要另外启动一个spark服务，会占用spark资源，现在改用hive的hiveserver2，修改了一些创表时，对应的类型描述，如spark中integer和long，转为hive识别的int
#20181031
1.添加了ACCUMULATE_WITH_PERIOD功能，按需求累加计算
2.添加了nillfill填补缺失值的功能
3.join outer时，两主键列自动补全
4.优化了一下循环的代码
#20181107
1.添加了QUARTER，YEAR_MONTH，YEAR_MONTH_DAY，YEAR_WEEK_RANGE函数，功能看文档(http://192.168.15.51:8181/docs/bi_web/122)
#20181123
1.增加了pivot转置功能（json格式参照pivot.txt），由于每次转置时，表结构都可能会变，所以在output时，会针对有pivot的输出表，先drop原有的table