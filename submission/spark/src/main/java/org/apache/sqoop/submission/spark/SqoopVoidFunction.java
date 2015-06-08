package org.apache.sqoop.submission.spark;


import org.apache.spark.api.java.function.VoidFunction;
// does nothing
public class SqoopVoidFunction implements VoidFunction<Integer> {
  private static final long serialVersionUID = 1L;

  private static SqoopVoidFunction instance = new SqoopVoidFunction();

  public static SqoopVoidFunction getInstance() {
    return instance;
  }

  private SqoopVoidFunction() {
  }

  @Override
  public void call(Integer t) throws Exception {
  }

}
