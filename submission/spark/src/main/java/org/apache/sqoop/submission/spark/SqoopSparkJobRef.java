package org.apache.sqoop.submission.spark;


public interface SqoopSparkJobRef {

  public String getJobId();

  public SqoopSparkJobStatus getSparkJobStatus();

  public boolean cancelJob();

}
