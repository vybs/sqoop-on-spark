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
package org.apache.sqoop.connector;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.core.ConfigurationConstants;
import org.apache.sqoop.error.code.ConnectorError;

/**
 * Utilities for ConnectorManager.
 */
public class ConnectorManagerUtils {

  /**
   * Get a list of URLs of connectors that are installed.
   *
   * @return List of URLs.
   */
  public static List<URL> getConnectorConfigs() {
    List<URL> connectorConfigs = new ArrayList<URL>();

    try {
      // Check ConnectorManager classloader.
      Enumeration<URL> appPath0Configs = ConnectorManager.class.getClassLoader().getResources(
          ConfigurationConstants.FILENAME_CONNECTOR_KITE_PROPERTIES);

      // TODO: fix the following temp hack to look up per property name
      Enumeration<URL> appPath1Configs = ConnectorManager.class.getClassLoader().getResources(
          ConfigurationConstants.FILENAME_CONNECTOR_JDBC_PROPERTIES);

      Enumeration<URL> appPath2Configs = ConnectorManager.class.getClassLoader().getResources(
          ConfigurationConstants.FILENAME_CONNECTOR_HDFS_PROPERTIES);

      Enumeration<URL> appPath3Configs = ConnectorManager.class.getClassLoader().getResources(
          ConfigurationConstants.FILENAME_CONNECTOR_KAFKA_PROPERTIES);

      while (appPath0Configs.hasMoreElements()) {
        connectorConfigs.add(appPath0Configs.nextElement());
      }
      while (appPath1Configs.hasMoreElements()) {
        connectorConfigs.add(appPath1Configs.nextElement());
      }

      while (appPath2Configs.hasMoreElements()) {
        connectorConfigs.add(appPath2Configs.nextElement());
      }
      while (appPath3Configs.hasMoreElements()) {
        connectorConfigs.add(appPath3Configs.nextElement());
      }

      // Check thread context classloader.
      ClassLoader ctxLoader = Thread.currentThread().getContextClassLoader();
      if (ctxLoader != null) {
        // TODO: fix the following temp hack to look up per property name

        Enumeration<URL> ctxPath0Configs = ctxLoader
            .getResources(ConfigurationConstants.FILENAME_CONNECTOR_KITE_PROPERTIES);

        Enumeration<URL> ctxPath1Configs = ctxLoader
            .getResources(ConfigurationConstants.FILENAME_CONNECTOR_JDBC_PROPERTIES);
        Enumeration<URL> ctxPath2Configs = ctxLoader
            .getResources(ConfigurationConstants.FILENAME_CONNECTOR_HDFS_PROPERTIES);
        Enumeration<URL> ctxPath3Configs = ctxLoader
            .getResources(ConfigurationConstants.FILENAME_CONNECTOR_HDFS_PROPERTIES);

         addToConnectorConfigs(connectorConfigs, ctxPath0Configs);
        addToConnectorConfigs(connectorConfigs, ctxPath1Configs);
        addToConnectorConfigs(connectorConfigs, ctxPath2Configs);
        addToConnectorConfigs(connectorConfigs, ctxPath3Configs);
      }
    } catch (IOException ex) {
      throw new SqoopException(ConnectorError.CONN_0001, ex);
    }

    return connectorConfigs;
  }

  private static void addToConnectorConfigs(List<URL> connectorConfigs,
      Enumeration<URL> ctxPathConfigs) {
    while (ctxPathConfigs.hasMoreElements()) {
      URL configUrl = ctxPathConfigs.nextElement();
      if (!connectorConfigs.contains(configUrl)) {
        connectorConfigs.add(configUrl);
      }
    }
  }

  public static Set<File> getConnectorJars(String path) {
    if (StringUtils.isEmpty(path)) {
      return null;
    }
    Set<File> jarFiles = new HashSet<File>();
    File folder = new File(path);
    if (folder.exists()) {
      for (File file : folder.listFiles()) {
        if (file.isDirectory()) {
          jarFiles.addAll(getConnectorJars(file.getPath()));
        }
        if (file.getName().endsWith(".jar") && isConnectorJar(file)) {
          jarFiles.add(file);
        }
      }
    }
    return jarFiles;
  }

  static boolean isConnectorJar(File file) {
    try {
      JarEntry entry = new JarFile(file)
          .getJarEntry(ConfigurationConstants.FILENAME_CONNECTOR_PROPERTIES);
      return entry != null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void addExternalConnectorsJarsToClasspath(String path) {
    if (StringUtils.isEmpty(path)) {
      return;
    }

    ClassLoader currentThreadClassLoader = Thread.currentThread().getContextClassLoader();
    if (currentThreadClassLoader != null) {

      // Add the 'org.apache.sqoop.connector.external.loadpath' to the classpath
      // Chain the current thread classloader
      ExternalConnectorJarFileLoader connectorUrlClassLoader = new ExternalConnectorJarFileLoader(
          new URL[] {}, currentThreadClassLoader);
      // the property always holds a path to the folder containing the jars
      Set<File> connectorJars = getConnectorJars(path);
      if (connectorJars != null && !connectorJars.isEmpty()) {
        for (File jar : connectorJars) {
          connectorUrlClassLoader.addJarFile(jar.getPath());
        }

        // Replace the thread classloader- assuming there is permission to do so
        Thread.currentThread().setContextClassLoader(connectorUrlClassLoader);
      }
    }
  }

  public static class ExternalConnectorJarFileLoader extends URLClassLoader {
    public ExternalConnectorJarFileLoader(URL[] urls, ClassLoader parent) {
      super(urls, parent);
    }

    public void addJarFile(String path) {
      String urlPath = "jar:file://" + path + "!/";
      try {
        addURL(new URL(urlPath));
      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }
    }

  }
}
