package bi.spark.etl.alludf.datefun

/*
补全outer_join后，主键的null值
+----+----+----+----+----+----+
|key1|key2|key3|key1|key2|key4|
+----+----+----+----+----+----+
|null|null|null| ddd|   3|   5|
| ccc|   3|   5|null|null|null|
| aaa|   1|   2| aaa|   2|   2|
| aaa|   1|   2| aaa|   1|   5|
| bbb|   3|   4| bbb|   3|   5|
| bbb|   3|   4| bbb|   4|   6|
| bbb|   4|   6| bbb|   3|   5|
| bbb|   4|   6| bbb|   4|   6|
|null|null|null| fff|   5|   6|
|null|null|null| eee|   1|   2|
+----+----+----+----+----+----+

+----+----+----+----+----+----+
|key1|key2|key3|key1|key2|key4|
+----+----+----+----+----+----+
| ddd|null|null| ddd|   3|   5|
| ccc|   3|   5| ccc|null|null|
| aaa|   1|   2| aaa|   2|   2|
| aaa|   1|   2| aaa|   1|   5|
| bbb|   3|   4| bbb|   3|   5|
| bbb|   3|   4| bbb|   4|   6|
| bbb|   4|   6| bbb|   3|   5|
| bbb|   4|   6| bbb|   4|   6|
| fff|null|null| fff|   5|   6|
| eee|null|null| eee|   1|   2|
+----+----+----+----+----+----+
 */

object nullFill {
  val valueNullFill : (String,String) => String = (left:String, right:String) => {
    if(left == null){right}
    else if (right == null) {left}
//    else if(left == right){left}
    else {left}
  }
}
