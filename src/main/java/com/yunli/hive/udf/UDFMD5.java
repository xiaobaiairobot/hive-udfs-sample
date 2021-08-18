package com.yunli.hive.udf;

import java.math.BigInteger;
import java.security.MessageDigest;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 计算输入参数的md5值。
 *
 * 创建临时函数：
 * add jar /tmp/hive-udfs-1.0-SNAPSHOT.jar;
 * create temporary function my_udf_md5 as 'com.yunli.hive.udf.UDFMD5';
 *
 * 创建永久函数：
 * hdfs dfs -put -f /tmp/hive-udfs-1.0-SNAPSHOT.jar /hive/libs
 * CREATE FUNCTION my_udf_md5 AS 'com.yunli.hive.udf.UDFMD5' USING JAR 'hdfs:///hive/libs/hive-udfs-1.0-SNAPSHOT.jar';
 *
 * 测试：
 * SELECT default.my_udf_md5('mytest');
 * 结果：
 * a599d36c4c7a71ddcc1bc7259a15ac3a
 */
@Description(name = "udfMD5", value = "_FUNC_(string) - MD5", extended = "Example:\n"
    + "  > SELECT my_udf_md5('mytst');\n")
public class UDFMD5 extends UDF {
  /**
   * 得到输入字符串的md5值
   * @param message 输入的字符串
   * @return 字符串对应的md5值
   */
  public String evaluate(String message) {
    if (message == null || message.length() == 0) {
      return null;
    }
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.update(message.getBytes());
      byte[] messageDigest = md.digest();
      BigInteger msgInt = new BigInteger(1, messageDigest);
      StringBuilder hashString = new StringBuilder(msgInt.toString(16));
      while (hashString.length() < 32) {
        // pre-pend with zeros if necessary
        hashString.insert(0, "0");
      }
      return hashString.toString();
    } catch (Exception e) {
      // If MD5 is not available
      return null;
    }
  }
}