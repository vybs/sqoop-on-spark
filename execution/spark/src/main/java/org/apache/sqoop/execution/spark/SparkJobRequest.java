package org.apache.sqoop.execution.spark;

import org.apache.sqoop.driver.JobRequest;
import java.io.Serializable;

/**
 * Custom data required for the spark job request
 *
 */
public class SparkJobRequest extends JobRequest implements Serializable {

	/**
   * 
   */
  private static final long serialVersionUID = 1L;

  public SparkJobRequest() {
		super();
	}

}
