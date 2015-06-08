package org.apache.sqoop.job.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.sqoop.common.Direction;

class SqoopOutputCommitter extends OutputCommitter {
  @Override
  public void setupJob(JobContext jobContext) {
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    super.commitJob(jobContext);
    invokeDestroyerExecutor(jobContext, true);
  }

  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
    super.abortJob(jobContext, state);
    invokeDestroyerExecutor(jobContext, false);
  }

  private void invokeDestroyerExecutor(JobContext jobContext, boolean success) {
    Configuration config = jobContext.getConfiguration();
    DestroyerUtil.executeDestroyer(success, config, Direction.FROM);
    DestroyerUtil.executeDestroyer(success, config, Direction.TO);
  }

  @Override
  public void setupTask(TaskAttemptContext taskContext) {
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) {
  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) {
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext) {
    return false;
  }
}
