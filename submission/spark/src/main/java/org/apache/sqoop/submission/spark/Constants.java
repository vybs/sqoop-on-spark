package org.apache.sqoop.submission.spark;

/**
 * Configuration constants for spark submission engine
 */
public class Constants {

  public static final String PREFIX_MAPREDUCE = "spark.";

  public static final String CONF_CONFIG_DIR =
    PREFIX_MAPREDUCE + "configuration.directory";

  public static final String SQOOP_JOB = "sqoop.job";

  private Constants() {
    // Instantiation is prohibited
  }
}
