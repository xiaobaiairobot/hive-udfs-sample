package com.yunli.hive.udf;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * 计算平均数
 *
 * 创建临时函数：
 * add jar /tmp/hive-udfs-1.0-SNAPSHOT.jar;
 * create temporary function my_avg as 'com.yunli.hive.udf.UDAFAvg';
 *
 * 创建永久函数：
 * hdfs dfs -put -f /tmp/hive-udfs-1.0-SNAPSHOT.jar /hive/libs
 * CREATE FUNCTION my_avg AS 'com.yunli.hive.udf.UDAFAvg' USING JAR 'hdfs:///hive/libs/hive-udfs-1.0-SNAPSHOT.jar';
 *
 * 测试：
 * hive> desc staff;
 * id                  	int
 * corporation_id      	int
 * name                	string
 * age                 	int
 *
 * hive> select * from staff;
 * 1	1	张三	30
 * 2	1	NULL	40
 * 3	2	李四	NULL
 * 4	2	王五	50
 * 5	3	陈六	18
 * 6	NULL	NULL	NULL
 * 7	3	郑七	18
 * 8	1	贾八	66
 * 9	1	zyy--	30
 * 100	1	angle	99999
 * 1	1	张三	30
 * 2	1	NULL	40
 * 3	2	李四	NULL
 * 4	2	王五	50
 * 5	3	陈六	18
 * 6	NULL	NULL	NULL
 * 7	3	郑七	18
 * 8	1	贾八	66
 *
 * hive> SELECT default.my_avg(age) from staff;
 * 结果：
 * 7176.642857142857
 *
 * hive> SELECT name,default.my_avg(age) from staff group by name;
 * 结果：
 * NULL	40.0
 * angle	99999.0
 * zyy--	30.0
 * 张三	30.0
 * 李四	NaN
 * 王五	50.0
 * 贾八	66.0
 * 郑七	18.0
 * 陈六	18.0
 */
@Description(name = "avg",
    value = "_FUNC_(x) - Returns an average all the elements in the aggregation group ",
    extended = "Example:\n  > SELECT _FUNC_(field1) FROM src;\n "
)

public class UDAFAvg implements GenericUDAFResolver2 {
  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
    ObjectInspector[] parameters = info.getParameterObjectInspectors();
    // 1. 参数个数校验
    if (parameters.length != 1) {
      throw new UDFArgumentException("Only one parameter is accepted.");
    }
    // 2. 参数类型校验
    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE ||
        ((PrimitiveObjectInspector) parameters[0]).getPrimitiveCategory()
            != PrimitiveObjectInspector.PrimitiveCategory.INT) {
      throw new UDFArgumentException("The parameter type must be int");
    }
    // 3. UDAF核心逻辑实现类
    return new AvgEvaluator();
  }

  /**
   * 该方法是用于兼容老的UDAF接口，不用实现
   * 如果通过 AbstractGenericUDAFResolver 实现 Resolver，则该方法作为 UDAF 的入口
   */
  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    throw new UDFArgumentException("方法未实现");
  }

  public static class AvgEvaluator extends GenericUDAFEvaluator {

    /**
     * 聚合过程中，用于保存中间结果的 Buffer
     * 继承 AbstractAggregationBuffer
     * <p>
     * 对于计算平均数，我们首先要计算总和(sum)和总数(count)
     * 最后用 总和 / 总数 就可以得到平均数
     */
    private static class AvgBuffer extends AbstractAggregationBuffer {
      // 总和
      private Integer sum = 0;

      // 总数
      private Integer count = 0;
    }

    /**
     * 初始化
     *
     * @param m          聚合模式
     * @param parameters 上一个阶段传过来的参数，可以在这里校验参数：
     *                   在 PARTIAL1 和 COMPLETE 模式，代表原始数据
     *                   在 PARTIAL2 和 FINAL 模式，代表部分聚合结果
     * @return 该阶段最终的返回值类型
     * 在 PARTIAL1 和 PARTIAL2 模式，代表 terminatePartial() 的返回值类型
     * 在 FINAL 和 COMPLETE 模式，代表 terminate() 的返回值类型
     */
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
        // 在 PARTIAL1 和 PARTIAL2 模式，代表 terminatePartial() 的返回值类型
        // terminatePartial() 返回的是部分聚合结果，这时候需要传递 sum 和 count，所以返回类型是结构体
        List<ObjectInspector> structFieldObjectInspectors = new LinkedList<ObjectInspector>();
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(
            Arrays.asList("sum", "count"),
            structFieldObjectInspectors
        );
      } else {
        // 在 FINAL 和 COMPLETE 模式，代表 terminate() 的返回值类型
        // 该函数最终返回一个 double 类型的数据，所以这里的返回类型是 double
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
      }
    }

    /**
     * 获取一个新的 Buffer，用于保存中间计算结果
     */
    @Override
    public GenericUDAFEvaluator.AggregationBuffer getNewAggregationBuffer() throws HiveException {
      // 直接实例化一个 AvgBuffer
      return new AvgBuffer();
    }

    /**
     * 重置 Buffer，在 Hive 程序执行时，可能会复用 Buffer 实例
     *
     * @param agg 被重置的 Buffer
     */
    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      // 重置 AvgBuffer 实例的状态
      ((AvgBuffer) agg).sum = 0;
      ((AvgBuffer) agg).count = 0;
    }

    /**
     * 读取原始数据，计算部分聚合结果
     *
     * @param agg        用于保存中间结果
     * @param parameters 原始数据
     */
    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      if (parameters == null || parameters[0] == null) {
        return;
      }

      if (parameters[0] instanceof IntWritable) {
        // 计算总和
        ((AvgBuffer) agg).sum += ((IntWritable) parameters[0]).get();
        // 计算总数
        ((AvgBuffer) agg).count += 1;
      }
    }

    /**
     * 输出部分聚合结果
     *
     * @param agg 保存的中间结果
     * @return 部分聚合结果，不一定是一个简单的值，可能是一个复杂的结构体
     */
    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      // 传递中间结果时，必须传递 总和、总数
      // 这里需要返回一个数组，表示结构体
      return new Object[] {
          new IntWritable(((AvgBuffer) agg).sum),
          new IntWritable(((AvgBuffer) agg).count)
      };
    }

    /**
     * 合并部分聚合结果
     * 输入：部分聚合结果
     * 输出：部分聚合结果
     *
     * @param agg     当前聚合中间结果类
     * @param partial 其他部分聚合结果值
     */
    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        // 传递过来的结构体为 LazyBinaryStruct 类型，需要从中提取数据
        ((AvgBuffer) agg).sum += ((IntWritable) ((LazyBinaryStruct) partial).getField(0)).get();
        ((AvgBuffer) agg).count += ((IntWritable) ((LazyBinaryStruct) partial).getField(1)).get();
      }
    }

    /**
     * 输出全局聚合结果
     *
     * @param agg 保存的中间结果
     */
    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      // 总和 / 总数
      return new DoubleWritable(1.0 * ((AvgBuffer) agg).sum / ((AvgBuffer) agg).count);
    }
  }
}
