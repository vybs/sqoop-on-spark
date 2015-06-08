package org.apache.sqoop.job.mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.connector.matcher.Matcher;
import org.apache.sqoop.error.code.MRExecutionError;
import org.apache.sqoop.etl.io.DataWriter;

//There are two IDF objects we carry around in memory during the sqoop job execution.
// The fromIDF has the fromSchema in it, the toIDF has the toSchema in it.
// Before we do the writing to the toIDF object we do the matching process to negotiate between
// the two schemas and their corresponding column types before we write the data to the toIDF object
public class SqoopDataWriter extends DataWriter {

  public static final Logger LOG = Logger.getLogger(SqoopDataWriter.class);

  private Context context;
  private IntermediateDataFormat<Object> fromIDF;
  private IntermediateDataFormat<Object> toIDF;
  private Matcher matcher;

  public SqoopDataWriter(Context context, IntermediateDataFormat<Object> f,
      IntermediateDataFormat<Object> t, Matcher m) {
    this.context = context;
    fromIDF = f;
    toIDF = t;
    matcher = m;

  }

  @Override
  public void writeArrayRecord(Object[] array) {
    fromIDF.setObjectData(array);
    writeContent();
  }

  @Override
  public void writeStringRecord(String text) {
    fromIDF.setCSVTextData(text);
    writeContent();
  }

  @Override
  public void writeRecord(Object obj) {
    fromIDF.setData(obj);
    writeContent();
  }

  private void writeContent() {
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Extracted data: " + fromIDF.getCSVTextData());
      }
      // NOTE: The fromIDF and the corresponding fromSchema is used only for the
      // matching process
      // The output of the mappers is finally written to the toIDF object after
      // the matching process
      // since the writable encapsulates the toIDF ==> new SqoopWritable(toIDF)
      toIDF.setObjectData(matcher.getMatchingData(fromIDF.getObjectData()));
      // NOTE: We do not use the reducer to do the writing (a.k.a LOAD in ETL).
      // Hence the mapper sets up the writable
      context.write(new Text(toIDF.getCSVTextData()), NullWritable.get());
    } catch (Exception e) {
      throw new SqoopException(MRExecutionError.MAPRED_EXEC_0013, e);
    }
  }
}
