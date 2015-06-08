package org.apache.sqoop.spark;

import java.io.Serializable;
import java.util.ListIterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.core.ConfigurationConstants;
import org.apache.sqoop.core.SqoopServer;
import org.apache.sqoop.driver.JobManager;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.job.spark.SparkDestroyerUtil;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.request.HttpEventContext;
import org.apache.sqoop.submission.spark.SqoopSparkDriver;

public class SqoopSparkJob implements Serializable {

  private MJob job;
  private static CommandLineParser parser;

  static class SqoopGnuParser extends GnuParser {

    private final boolean ignoreUnrecognizedOption;

    public SqoopGnuParser(final boolean ignoreUnrecognizedOption) {
      this.ignoreUnrecognizedOption = ignoreUnrecognizedOption;
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void processOption(final String arg, final ListIterator iter) throws ParseException {
      boolean hasOption = getOptions().hasOption(arg);

      // this allows us to parse the options for command and then parse again
      // based on command
      if (hasOption || !ignoreUnrecognizedOption) {
        super.processOption(arg, iter);
      }
    }

  }

  SqoopSparkJob() {
    parser = new SqoopGnuParser(true);

  }

  public void setJob(MJob job) {
    this.job = job;
  }

  public static CommandLine parseArgs(Options options, String[] args) {

    CommandLine commandLineArgs;
    try {
      // parse the command line arguments
      commandLineArgs = parser.parse(options, args, false);
    } catch (ParseException pe) {
      throw new RuntimeException("Parsing failed for command option:", pe);
    }
    return commandLineArgs;
  }

  @SuppressWarnings("static-access")
  public static void addCommonOptions(Options options) {
    options.addOption(OptionBuilder.withLongOpt("numE").withDescription("extractor parallelism")
        .hasArg().isRequired().withArgName("numExtractors").create());

    options.addOption(OptionBuilder.withLongOpt("numL").withDescription("loader parallelism")
        .hasArg().isRequired().withArgName("numLoaders").create());

    options.addOption(OptionBuilder.withLongOpt("confDir").withDescription("config dir for sqoop")
        .hasArg().isRequired().withArgName("confDir").create());
  }

  public SparkConf init(CommandLine cArgs) throws ClassNotFoundException {
    System.setProperty(ConfigurationConstants.SYSPROP_CONFIG_DIR, cArgs.getOptionValue("confDir"));
    // by default it is local, override based on the submit parameter
    SparkConf conf = new SparkConf().setAppName("sqoop-spark").setMaster("local");
    conf.set(SqoopSparkDriver.NUM_EXTRACTERS, cArgs.getOptionValue("numE"));
    conf.set(SqoopSparkDriver.NUM_LOADERS, cArgs.getOptionValue("numL"));
    // hack to load extra classes
    Class.forName("com.mysql.jdbc.Driver");
    SqoopServer.initialize();
    return conf;
  }

  public void execute(SparkConf conf, JavaSparkContext context) throws Exception {

    if (job == null) {
      throw new RuntimeException("Job not set for spark execution");
    }
    HttpEventContext ctx = new HttpEventContext();
    ctx.setUsername(conf.get("username") != null ? conf.get("username") : "testsqoop");
    MSubmission mSubmission = JobManager.getInstance().createJobSubmission(ctx,
        job.getPersistenceId());
    JobRequest jobRequest = JobManager.getInstance().createJobRequest(job.getPersistenceId(),
        mSubmission);
    JobManager.getInstance().prepareJob(jobRequest);
    SqoopSparkDriver.run(jobRequest, conf, context);
    SparkDestroyerUtil.executeDestroyer(true, jobRequest, Direction.FROM);
    SparkDestroyerUtil.executeDestroyer(true, jobRequest, Direction.TO);
    SqoopServer.destroy();
  }
}
