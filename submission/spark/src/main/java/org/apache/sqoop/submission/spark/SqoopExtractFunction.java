package org.apache.sqoop.submission.spark;

import java.io.Serializable;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.connector.matcher.Matcher;
import org.apache.sqoop.connector.matcher.MatcherFactory;
import org.apache.sqoop.core.JobConstants;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.error.code.MRExecutionError;
import org.apache.sqoop.job.SparkPrefixContext;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.utils.ClassUtils;

@SuppressWarnings("serial")
public class SqoopExtractFunction implements Function<Partition, List<IntermediateDataFormat<?>>>, Serializable {

  private final JobRequest request;

  public static final Logger LOG = Logger.getLogger(SqoopExtractFunction.class);

  public SqoopExtractFunction(JobRequest request) {
    this.request = request;
  }

  @Override
  public List<IntermediateDataFormat<?>> call(Partition p) throws Exception {

    long mapTime = System.currentTimeMillis();
    String extractorName = request.getDriverContext().getString(JobConstants.JOB_ETL_EXTRACTOR);

    Extractor extractor = (Extractor) ClassUtils.instantiate(extractorName);

    Schema fromSchema = request.getJobSubmission().getFromSchema();
    
    Schema toSchema = request.getJobSubmission().getToSchema();
    
    Matcher matcher = MatcherFactory.getMatcher(fromSchema, toSchema);

    String fromIDFClass = request.getDriverContext().getString(
        JobConstants.FROM_INTERMEDIATE_DATA_FORMAT);
    IntermediateDataFormat<Object> fromIDF = (IntermediateDataFormat<Object>) ClassUtils
        .instantiate(fromIDFClass);
    fromIDF.setSchema(matcher.getFromSchema());
    
    
    String toIDFClass = request.getDriverContext().getString(
        JobConstants.TO_INTERMEDIATE_DATA_FORMAT);
    IntermediateDataFormat<Object> toIDF = (IntermediateDataFormat<Object>) ClassUtils
        .instantiate(toIDFClass);
    toIDF.setSchema(matcher.getToSchema());

    // Objects that should be passed to the Executor execution
    SparkPrefixContext subContext = new SparkPrefixContext(request.getConf(),
        JobConstants.PREFIX_CONNECTOR_FROM_CONTEXT);

    Object fromLinkConfig = request.getConnectorLinkConfig(Direction.FROM);
    Object fromJobConfig = request.getJobConfig(Direction.FROM);

    ExtractorContext extractorContext = new ExtractorContext(subContext, new SparkDataWriter(
        request, fromIDF, toIDF, matcher), fromSchema);

    try {
      System.out.println("Starting extractor... ");
      extractor.extract(extractorContext, fromLinkConfig, fromJobConfig, p);

    } catch (Exception e) {
      throw new SqoopException(MRExecutionError.MAPRED_EXEC_0017, e);
    } finally {
      LOG.info("Stopping progress service");
    }

    System.out.println("Extractor has finished");
    System.out.println(">>> MAP time ms:"+(System.currentTimeMillis()-mapTime));

    return request.getData();
  }

}
