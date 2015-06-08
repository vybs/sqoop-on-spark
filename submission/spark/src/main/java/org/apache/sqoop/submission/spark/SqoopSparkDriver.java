package org.apache.sqoop.submission.spark;

import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.core.JobConstants;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.error.code.SparkExecutionError;
import org.apache.sqoop.job.SparkPrefixContext;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.job.etl.Partitioner;
import org.apache.sqoop.job.etl.PartitionerContext;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.utils.ClassUtils;

public class SqoopSparkDriver {

  public static final String NUM_EXTRACTERS = "numExtractors";
  public static final String NUM_LOADERS = "numLoaders";


  private static final Log LOG = LogFactory.getLog(SqoopSparkDriver.class.getName());

   public static void run(JobRequest request, SparkConf conf, JavaSparkContext sc) throws Exception {
   
    LOG.info("Executing sqoop spark job");

    long totalTime = System.currentTimeMillis();
    
    List<Partition> sp = getPartitions(request, conf.getInt("numExtractors", 1));
    JavaRDD<Partition> rdd = sc.parallelize(sp, sp.size());
    JavaRDD<Collection<IntermediateDataFormat<?>>> mapRDD = rdd.map(new SqoopExtractFunction(request));
    mapRDD.map(new SqoopLoadFunction(request)).collect();

    System.out.println(">>> TOTAL time ms:"+ (System.currentTimeMillis()-totalTime));
    System.out.println(">>> Partition size" + sp.size());

    LOG.info("Done EL in sqoop spark job, nexy destroy");

  }

  private static List<Partition> getPartitions(JobRequest request, int numMappers) {
    String partitionerName = request.getDriverContext().getString(JobConstants.JOB_ETL_PARTITIONER);
    Partitioner partitioner = (Partitioner) ClassUtils.instantiate(partitionerName);
    SparkPrefixContext context = new SparkPrefixContext(request.getConf(),
        JobConstants.PREFIX_CONNECTOR_FROM_CONTEXT);

    Object fromLinkConfig = request.getConnectorLinkConfig(Direction.FROM);
    Object fromJobConfig = request.getJobConfig(Direction.FROM);
    Schema fromSchema = request.getJobSubmission().getFromSchema();
    
    long maxPartitions = context.getLong(JobConstants.JOB_ETL_EXTRACTOR_NUM, numMappers);
    PartitionerContext partitionerContext = new PartitionerContext(context, maxPartitions,
        fromSchema);

    List<Partition> partitions = partitioner.getPartitions(partitionerContext, fromLinkConfig,
        fromJobConfig);

    if (partitions.size() > maxPartitions) {
      throw new SqoopException(SparkExecutionError.SPARK_EXEC_0000, String.format(
          "Got %d, max was %d", partitions.size(), maxPartitions));
    }
    return partitions;
  }
}
