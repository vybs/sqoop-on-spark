package org.apache.sqoop.submission.spark;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.connector.matcher.Matcher;
import org.apache.sqoop.connector.matcher.MatcherFactory;
import org.apache.sqoop.core.JobConstants;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.error.code.SparkExecutionError;
import org.apache.sqoop.etl.io.DataReader;
import org.apache.sqoop.job.SparkPrefixContext;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.utils.ClassUtils;

@SuppressWarnings("serial")
public class SqoopLoadFunction implements
    FlatMapFunction<Iterator<List<IntermediateDataFormat<?>>>, Void>, Serializable {

  private final JobRequest request;

  public static final Logger LOG = Logger.getLogger(SqoopLoadFunction.class);

  public SqoopLoadFunction(JobRequest request) {
    this.request = request;
  }

  @Override
  public Iterable<Void> call(Iterator<List<IntermediateDataFormat<?>>> data) throws Exception {

    long reduceTime = System.currentTimeMillis();

    String loaderName = request.getDriverContext().getString(JobConstants.JOB_ETL_LOADER);
    Schema fromSchema = request.getJobSubmission().getFromSchema();
    Schema toSchema = request.getJobSubmission().getToSchema();
    Matcher matcher = MatcherFactory.getMatcher(fromSchema, toSchema);

    LOG.info("Sqoop Load Function is  starting");
    try {
      while (data.hasNext()) {
        DataReader reader = new SparkDataReader(data.next());

        Loader loader = (Loader) ClassUtils.instantiate(loaderName);

        SparkPrefixContext subContext = new SparkPrefixContext(request.getConf(),
            JobConstants.PREFIX_CONNECTOR_TO_CONTEXT);

        Object toLinkConfig = request.getConnectorLinkConfig(Direction.TO);
        Object toJobConfig = request.getJobConfig(Direction.TO);

        // Create loader context
        LoaderContext loaderContext = new LoaderContext(subContext, reader, matcher.getToSchema());

        LOG.info("Running loader class " + loaderName);
        loader.load(loaderContext, toLinkConfig, toJobConfig);
        System.out.println("Loader has finished");
        System.out.println(">>> REDUCE time ms:" + (System.currentTimeMillis() - reduceTime));
      }
    } catch (Throwable t) {
      LOG.error("Error while loading data out of MR job.", t);
      throw new SqoopException(SparkExecutionError.SPARK_EXEC_0000, t);
    }

    return Collections.singletonList(null);

  }

}
