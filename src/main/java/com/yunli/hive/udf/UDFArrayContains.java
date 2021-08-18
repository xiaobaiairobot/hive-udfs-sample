package com.yunli.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

/**
 * 字符串集合中是否包含某个字符串
 *
 * 创建临时函数：
 * add jar /tmp/hive-udfs-1.0-SNAPSHOT.jar;
 * create temporary function my_array_contains as 'com.yunli.hive.udf.UDFArrayContains';
 *
 * 创建永久函数：
 * hdfs dfs -put -f /tmp/hive-udfs-1.0-SNAPSHOT.jar /hive/libs
 * CREATE FUNCTION my_array_contains AS 'com.yunli.hive.udf.UDFArrayContains' USING JAR 'hdfs:///hive/libs/hive-udfs-1.0-SNAPSHOT.jar';
 *
 * 测试：
 * SELECT default.my_array_contains(array('张三','李四','王五'),'李四');
 * 结果：
 * ture
 */
@Description(name = "array_contains", value = "_FUNC_(array, value) - Returns TRUE if the array contains value.",
    extended = "Example:\n  > SELECT _FUNC_(array(1, 2, 3),2) FROM src LIMIT 1;\n  true")
public class UDFArrayContains extends GenericUDF {

  private transient ObjectInspector value;

  private transient ListObjectInspector array;

  private transient ObjectInspector arrayElement;

  private BooleanWritable result;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentException("The function ARRAY_CONTAINS accepts 2 arguments.");
    }

    if (!(arguments[0].getCategory().equals(ObjectInspector.Category.LIST))) {
      throw new UDFArgumentTypeException(0, "\"array\" expected at function ARRAY_CONTAINS, but \""
          + arguments[0].getTypeName() + "\" " + "is found");
    }

    this.array = ((ListObjectInspector) arguments[0]);
    this.arrayElement = this.array.getListElementObjectInspector();

    this.value = arguments[1];

    if (!(ObjectInspectorUtils.compareTypes(this.arrayElement, this.value))) {
      throw new UDFArgumentTypeException(1,
          "\"" + this.arrayElement.getTypeName() + "\"" + " expected at function ARRAY_CONTAINS, but "
              + "\"" + this.value.getTypeName() + "\"" + " is found");
    }

    if (!(ObjectInspectorUtils.compareSupported(this.value))) {
      throw new UDFArgumentException("The function ARRAY_CONTAINS does not support comparison for \""
          + this.value.getTypeName() + "\"" + " types");
    }

    this.result = new BooleanWritable(false);

    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {
    this.result.set(false);

    Object array = arguments[0].get();
    Object value = arguments[1].get();

    int arrayLength = this.array.getListLength(array);

    if ((value == null) || (arrayLength <= 0)) {
      return this.result;
    }

    for (int i = 0; i < arrayLength; ++i) {
      Object listElement = this.array.getListElement(array, i);
      if ((listElement == null)
          || (ObjectInspectorUtils.compare(value, this.value, listElement, this.arrayElement) != 0)) {
        continue;
      }
      this.result.set(true);
      break;
    }
    return this.result;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 2);
    return "array_contains(" + children[0] + ", " + children[1] + ")";
  }
}