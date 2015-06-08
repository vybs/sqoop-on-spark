package org.apache.sqoop.integration.connector.spark;

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.testcases.ConnectorTestCase;
import org.testng.annotations.Test;

public class SparkJobTest extends ConnectorTestCase {

	@Test
	public void test() throws Exception {
		createAndLoadTableCities();

		// RDBMS link
		MLink rdbmsConnection = getClient()
				.createLink("generic-jdbc-connector");
		fillRdbmsLinkConfig(rdbmsConnection);
		saveLink(rdbmsConnection);

		// HDFS link
		MLink hdfsConnection = getClient().createLink("hdfs-connector");
		fillHdfsLink(hdfsConnection);
		saveLink(hdfsConnection);

		// Job creation
		MJob job = getClient().createJob(rdbmsConnection.getPersistenceId(),
				hdfsConnection.getPersistenceId());

		// Set rdbms "FROM" config
		fillRdbmsFromConfig(job, "id");

		// Fill the hdfs "TO" config
		fillHdfsToConfig(job, ToFormat.TEXT_FILE);
		MConfigList toConfig = job.getJobConfig(Direction.TO);
		toConfig.getBooleanInput("toJobConfig.appendMode").setValue(true);

		saveJob(job);
		
	    // First execution
	    executeJob(job);

	}
}