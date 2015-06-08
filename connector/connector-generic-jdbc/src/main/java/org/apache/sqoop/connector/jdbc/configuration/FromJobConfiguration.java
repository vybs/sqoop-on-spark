
package org.apache.sqoop.connector.jdbc.configuration;
import org.apache.sqoop.connector.spi.BaseConfig;

import org.apache.sqoop.model.ConfigurationClass;
import org.apache.sqoop.model.Config;

/**
 *
 */
@ConfigurationClass
public class FromJobConfiguration extends BaseConfig {
  @Config public FromJobConfig fromJobConfig;

  @Config public IncrementalRead incrementalRead;

  public FromJobConfiguration() {
    fromJobConfig = new FromJobConfig();
    incrementalRead = new IncrementalRead();
  }
}
