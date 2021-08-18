package com.yunli.hive.udf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * @Author: ming.xin
 * @Date: 2021/8/17 11:05
 * @Description: udfmd5的测试类
 **/
public class UDFMD5Test {

  /**
   * mytest的md5：a599d36c4c7a71ddcc1bc7259a15ac3a
   */
  @Test
  public void testEvaluate() {
    UDFMD5 udfmd5 = new UDFMD5();
    String myTest = udfmd5.evaluate("mytest");
    assertEquals("a599d36c4c7a71ddcc1bc7259a15ac3a", myTest);
  }

  @Test
  public void testEvaluate_empty() {
    UDFMD5 udfmd5 = new UDFMD5();
    String myTest = udfmd5.evaluate(null);
    assertNull(myTest);
  }
}