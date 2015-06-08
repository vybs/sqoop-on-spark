package org.apache.sqoop.job.mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SqoopRecordReader extends RecordReader<SqoopSplit, NullWritable> {

  private boolean delivered = false;
  private SqoopSplit split = null;

  @Override
  public boolean nextKeyValue() {
    if (delivered) {
      return false;
    } else {
      delivered = true;
      return true;
    }
  }

  @Override
  public SqoopSplit getCurrentKey() {
    return split;
  }

  @Override
  public NullWritable getCurrentValue() {
    return NullWritable.get();
  }

  @Override
  public void close() {
  }

  
  @Override
  public float getProgress() {
    return delivered ? 1.0f : 0.0f;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) {
    this.split = (SqoopSplit) split;
  }
}
