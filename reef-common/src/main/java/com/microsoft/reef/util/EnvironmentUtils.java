/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.util;

import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.OptionalParameter;
import com.microsoft.tang.formats.Param;

import java.io.File;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class EnvironmentUtils {

  private static final Logger LOG = Logger.getLogger(EnvironmentUtils.class.getName());

  /**
   * Get a set of all classpath entries EXCEPT of those under
   * $JAVA_HOME, $YARN_HOME, and $HADOOP_HOME.
   *
   * @return A set of classpath entries as strings.
   */
  public static Set<String> getAllClasspathJars() {
    return getAllClasspathJars("JAVA_HOME", "YARN_HOME", "HADOOP_HOME",
        "HADOOP_YARN_HOME", "HADOOP_COMMON_HOME", "HADOOP_MAPRED_HOME", "HADOOP_HDFS_HOME");
  }

  /**
   * Get a set of all classpath entries EXCEPT of those under excludeEnv directories.
   * Every excludeEnv entry is an environment variable name.
   *
   * @return A set of classpath entries as strings.
   */
  public static Set<String> getAllClasspathJars(final String... excludeEnv) {

    final Set<String> jars = new HashSet<>();
    final Set<Path> excludePaths = new HashSet<>();

    for (final String env : excludeEnv) {
      final String path = System.getenv(env);
      if (null != path) {
        final File file = new File(path);
        if (file.exists()) {
          excludePaths.add(file.toPath());
        }
      }
    }

    for (final String path : System.getProperty("java.class.path").split(File.pathSeparator)) {
      try {
        final File file = new File(path);
        if (file.exists()) {
          final Path absolutePath = file.toPath();
          boolean toBeAdded = true;
          for (final Path prefix : excludePaths) {
            if (absolutePath.startsWith(prefix)) {
              toBeAdded = false;
            }
          }
          if (toBeAdded) {
            jars.add(absolutePath.toString());
          }
        }
      } catch (final InvalidPathException ex) {
        LOG.log(Level.FINE, "Skip path: {0}: {1}", new Object[]{path, ex});
      }
    }

    return jars;
  }

  /**
   * @param config
   * @param param
   * @param values
   * @param <P>
   * @return
   * @deprecated in 0.2 this really should be in Tang.
   * See <a href="https://github.com/Microsoft-CISL/TANG/issues/164">Tang #164</a> for details.
   */
  @Deprecated
  public static <P extends Param> ConfigurationModule addAll(
      ConfigurationModule config, final P param, final Iterable<String> values) {
    for (final String val : values) {
      config = config.set(param, val);
    }
    return config;
  }

  /**
   * @param config
   * @param param
   * @return
   */
  public static ConfigurationModule addClasspath(
      final ConfigurationModule config, final OptionalParameter<String> param) {
    return addAll(config, param, getAllClasspathJars());
  }

  /**
   * Check whether assert() statements are evaluated.
   *
   * @return true, if assertions are enabled. False otherwise.
   */
  public static boolean areAssertionsEnabled() {
    try {
      assert false;
      // If we got here, the assert above can't have thrown an exception. hence, asserts must be off.
      return false;
    } catch (final AssertionError assertionError) {
      // The assert above threw an exception. Asserts must be enabled.
      return true;
    }
  }
}
