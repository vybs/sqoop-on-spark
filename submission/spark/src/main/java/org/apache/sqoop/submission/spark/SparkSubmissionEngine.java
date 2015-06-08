/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.submission.spark;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.log4j.Logger;
import org.apache.spark.SparkException;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.driver.SubmissionEngine;
import org.apache.sqoop.error.code.SparkSubmissionError;
import org.apache.sqoop.execution.spark.SparkExecutionEngine;
import org.apache.sqoop.execution.spark.SparkJobRequest;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.SubmissionError;

/**
 * This is very simple and straightforward implementation of spark submission
 * engine.
 */
public class SparkSubmissionEngine extends SubmissionEngine {

  private static Logger LOG = Logger.getLogger(SparkSubmissionEngine.class);

  // yarn config from yarn-site.xml

  // private Configuration yarnConfiguration;

  private SqoopSparkClient sparkClient;

  private SqoopConf sqoopConf;

  @Override
  public boolean isExecutionEngineSupported(Class<?> executionEngineClass) {
    return executionEngineClass == SparkExecutionEngine.class;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean submit(JobRequest jobRequest) {
    // We're supporting only map reduce jobs
    SparkJobRequest request = (SparkJobRequest) jobRequest;

    sqoopConf.add("spark.master", "local");// + request.getExtractors() + "]");

    // setYarnConfig(request);

    try {
      sparkClient.execute(jobRequest);
    } catch (Exception e) {
      SubmissionError error = new SubmissionError();
      error.setErrorSummary(e.toString());
      StringWriter writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      writer.flush();
      error.setErrorDetails(writer.toString());

      request.getJobSubmission().setError(error);
      LOG.error("Error in submitting job", e);
      return false;
    }

    return true;

  }

  @Override
  public void stop(String externalJobId) {
    LOG.info("Destroying Spark Submission Engine");
  }

  @Override
  public void update(MSubmission submission) {

    // not much can be done, since we do not have easy api to ping spark for
    // app stats from in process
  }

  @Override
  public void initialize(MapContext context, String prefix) {
    LOG.info("Initializing Spark Submission Engine");

    sqoopConf = new SqoopConf();
    // Load configured spark configuration directory
    // Create spark client, for now a local one
    try {

      sqoopConf.add(" spark.ui.enabled", "false");
      sqoopConf.add(" spark.driver.allowMultipleContexts", "true");

      sparkClient = SqoopSparkClientFactory.createSqoopSparkClient(sqoopConf);
    } catch (IOException e) {
      throw new SqoopException(SparkSubmissionError.SPARK_0002, e);
    } catch (SparkException e) {
      throw new SqoopException(SparkSubmissionError.SPARK_0002, e);
    }

  }

  @Override
  public void destroy() {

  }

}
