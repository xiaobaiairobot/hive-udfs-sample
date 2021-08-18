package com.yunli.hive.udf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 清洗人名
 *
 * 创建临时函数：
 * add jar /tmp/hive-udfs-1.0-SNAPSHOT.jar;
 * create temporary function my_name_parser as 'com.yunli.hive.udf.UDTFNameParser';
 *
 * 创建永久函数：
 * hdfs dfs -put -f /tmp/hive-udfs-1.0-SNAPSHOT.jar /hive/libs
 * CREATE FUNCTION my_name_parser AS 'com.yunli.hive.udf.UDTFNameParser' USING JAR 'hdfs:///hive/libs/hive-udfs-1.0-SNAPSHOT.jar';
 *
 * 测试：
 * hive> desc people;
 * name                	string
 *
 * hive> select * from people;
 * John Smith
 * John and Ann White
 * Ted Green
 * Dorothy
 *
 * hive> SELECT adTable.name,adTable.surname FROM people lateral view default.my_name_parser(name) adTable as name,surname;
 * 结果：
 * John	Smith
 * John	White
 * Ann	White
 * Ted	Green
 */
public class UDTFNameParser extends GenericUDTF {
  private PrimitiveObjectInspector stringOI = null;

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length != 1) {
      throw new UDFArgumentException("UDTFNameParser() takes exactly one argument");
    }

    if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
        && ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory()
        != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
      throw new UDFArgumentException("UDTFNameParser() takes a string as a parameter");
    }

    // input
    stringOI = (PrimitiveObjectInspector) args[0];

    // output
    List<String> fieldNames = new ArrayList<String>(2);
    List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(2);
    fieldNames.add("name");
    fieldNames.add("surname");
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  public ArrayList<Object[]> processInputRecord(String name) {
    ArrayList<Object[]> result = new ArrayList<Object[]>();

    // ignoring null or empty input
    if (name == null || name.isEmpty()) {
      return result;
    }

    String[] tokens = name.split("\\s+");

    if (tokens.length == 2) {
      result.add(new Object[] {tokens[0], tokens[1]});
    } else if (tokens.length == 4 && tokens[1].equals("and")) {
      result.add(new Object[] {tokens[0], tokens[3]});
      result.add(new Object[] {tokens[2], tokens[3]});
    }

    return result;
  }

  @Override
  public void process(Object[] record) throws HiveException {
    final String name = stringOI.getPrimitiveJavaObject(record[0]).toString();
    ArrayList<Object[]> results = processInputRecord(name);

    Iterator<Object[]> it = results.iterator();

    while (it.hasNext()) {
      Object[] r = it.next();
      forward(r);
    }
  }

  @Override
  public void close() throws HiveException {
    // do nothing
  }
}