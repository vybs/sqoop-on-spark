package org.apache.sqoop.submission.spark;

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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.job.spark.SparkDestroyerUtil;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

public class LocalSqoopSparkClient implements SqoopSparkClient {

  private static final long serialVersionUID = 1L;
  protected static final transient Log LOG = LogFactory.getLog(LocalSqoopSparkClient.class);

  private static final Splitter CSV_SPLITTER = Splitter.on(",").omitEmptyStrings();

  private static LocalSqoopSparkClient client;

  public static synchronized LocalSqoopSparkClient getInstance(SparkConf sparkConf) {
    if (client == null) {
      client = new LocalSqoopSparkClient(sparkConf);
    }
    return client;
  }

  private JavaSparkContext sc;

  private List<String> localJars = new ArrayList<String>();

  private List<String> localFiles = new ArrayList<String>();

  private LocalSqoopSparkClient(SparkConf sparkConf) {
   // = new JavaSparkContext(sparkConf);
  }

  public SparkConf getSparkConf() {
    return sc.sc().conf();
  }

  public int getExecutorCount() {
    return sc.sc().getExecutorMemoryStatus().size();
  }

  public int getDefaultParallelism() throws Exception {
    return sc.sc().defaultParallelism();
  }

  public void execute(JobRequest request) throws Exception {

    // SparkCounters sparkCounters = new SparkCounters(sc);
    SqoopSparkDriver.run(request, getSparkConf(), sc);
    SparkDestroyerUtil.executeDestroyer(true, request, Direction.FROM);
    SparkDestroyerUtil.executeDestroyer(true, request, Direction.TO);

  }

  public void addResources(String addedFiles) {
    for (String addedFile : CSV_SPLITTER.split(Strings.nullToEmpty(addedFiles))) {
      if (!localFiles.contains(addedFile)) {
        localFiles.add(addedFile);
        sc.addFile(addedFile);
      }
    }
  }

  private void addJars(String addedJars) {
    for (String addedJar : CSV_SPLITTER.split(Strings.nullToEmpty(addedJars))) {
      if (!localJars.contains(addedJar)) {
        localJars.add(addedJar);
        sc.addJar(addedJar);
      }
    }
  }

  public void close() {
    sc.stop();
    client = null;
  }
}
