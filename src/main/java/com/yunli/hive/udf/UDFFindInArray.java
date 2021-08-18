package com.yunli.hive.udf;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 字符串集合中是否包含某个字符串
 *
 * 创建临时函数：
 * add jar /tmp/hive-udfs-1.0-SNAPSHOT.jar;
 * create temporary function my_find_in_array as 'com.yunli.hive.udf.UDFFindInArray';
 *
 * 创建永久函数：
 * hdfs dfs -put -f /tmp/hive-udfs-1.0-SNAPSHOT.jar /hive/libs
 * CREATE FUNCTION my_find_in_array AS 'com.yunli.hive.udf.UDFFindInArray' USING JAR 'hdfs:///hive/libs/hive-udfs-1.0-SNAPSHOT.jar';
 *
 * 测试：
 * SELECT default.my_find_in_array('李四', array('张三','李四','王五'));
 * 结果：
 * 2
 */
@Description(name = "find_in_array",
    value = "_FUNC_(NEEDLE, HAYSTACK) - Find the first 1-indexed value of HAYSTACK which matches NEEDLE.  Returns NULL if HAYSTACK is NULL.  Returns 0 if NEEDLE is not found in HAYSTACK or is NULL.",
    extended = "Example:\n"
        + "  > SELECT _FUNC_(2, array(1, 2, 3)) FROM users;\n")
public class UDFFindInArray extends UDF {
  public Integer evaluate(String needle, List<String> haystack) {
    if (needle == null) {
      return 0;
    }
    if (haystack == null) {
      return null;
    }
    int retval = 0;
    for (int ii = 0; ii < haystack.size(); ++ii) {
      if (haystack.get(ii).equals(needle)) {
        retval = ii + 1;
      }
    }
    return retval;
  }
}