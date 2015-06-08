package org.apache.sqoop.execution.spark;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.core.JobConstants;
import org.apache.sqoop.driver.ExecutionEngine;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.job.SparkJobConstants;
import org.apache.sqoop.job.etl.From;
import org.apache.sqoop.job.etl.To;

public class SparkExecutionEngine extends ExecutionEngine {

  private static Logger LOG = Logger.getLogger(SparkExecutionEngine.class);

  @Override
  public JobRequest createJobRequest() {
    return new SparkJobRequest();
  }

  @Override
  public void prepareJob(JobRequest request) {

    SparkJobRequest sparkJobRequest = (SparkJobRequest) request;

    // Add jar dependencies anything extra needed
    addDependencies(sparkJobRequest);

    // set anything the spark job needs here

    From from = (From) sparkJobRequest.getFrom();
    To to = (To) sparkJobRequest.getTo();

    MutableMapContext driverContext = sparkJobRequest.getDriverContext();
    driverContext.setString(SparkJobConstants.JOB_ETL_PARTITIONER, from.getPartitioner().getName());
    driverContext.setString(SparkJobConstants.JOB_ETL_EXTRACTOR, from.getExtractor().getName());
    driverContext.setString(SparkJobConstants.JOB_ETL_LOADER, to.getLoader().getName());
    driverContext
        .setString(SparkJobConstants.JOB_ETL_FROM_DESTROYER, from.getDestroyer().getName());
    driverContext.setString(SparkJobConstants.JOB_ETL_TO_DESTROYER, to.getDestroyer().getName());
    driverContext.setString(SparkJobConstants.FROM_INTERMEDIATE_DATA_FORMAT, sparkJobRequest
        .getIntermediateDataFormat(Direction.FROM).getName());
    driverContext.setString(SparkJobConstants.TO_INTERMEDIATE_DATA_FORMAT, sparkJobRequest
        .getIntermediateDataFormat(Direction.TO).getName());

    if (sparkJobRequest.getExtractors() != null) {
      driverContext.setInteger(SparkJobConstants.JOB_ETL_EXTRACTOR_NUM,
          sparkJobRequest.getExtractors());
      
      System.out.println(">>> Configured Extractor size:" + sparkJobRequest.getExtractors());

    }

    for (Map.Entry<String, String> entry : request.getDriverContext()) {
      if (entry.getValue() == null) {
        LOG.warn("Ignoring null driver context value for key " + entry.getKey());
        continue;
      }
      request.getConf().put(entry.getKey(), entry.getValue());
    }

    // Serialize connector context as a sub namespace
    for (Map.Entry<String, String> entry : request.getConnectorContext(Direction.FROM)) {
      if (entry.getValue() == null) {
        LOG.warn("Ignoring null connector context value for key " + entry.getKey());
        continue;
      }
      request.getConf().put(JobConstants.PREFIX_CONNECTOR_FROM_CONTEXT + entry.getKey(),
          entry.getValue());
    }

    for (Map.Entry<String, String> entry : request.getConnectorContext(Direction.TO)) {
      if (entry.getValue() == null) {
        LOG.warn("Ignoring null connector context value for key " + entry.getKey());
        continue;
      }
      request.getConf().put(JobConstants.PREFIX_CONNECTOR_TO_CONTEXT + entry.getKey(),
          entry.getValue());
    }
    
    for (Map.Entry<String, String> entry : request.getDriverContext()) {
      if (entry.getValue() == null) {
        LOG.warn("Ignoring null connector context value for key " + entry.getKey());
        continue;
      }
      request.getConf().put(JobConstants.PREFIX_CONNECTOR_DRIVER_CONTEXT + entry.getKey(),
          entry.getValue());
    }
  }

  protected void addDependencies(SparkJobRequest jobrequest) {
    // not much to do here
  }
}
