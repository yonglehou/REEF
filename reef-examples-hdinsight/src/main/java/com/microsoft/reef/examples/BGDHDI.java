package com.microsoft.reef.examples;

import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.examples.nggroup.bgd.BGDClient;
import com.microsoft.reef.runtime.hdinsight.client.UnsafeHDInsightRuntimeConfiguration;
import com.microsoft.tang.Configuration;

/**
 * Runs BGD on HDInsight
 */
public class BGDHDI {
  public static void main(final String[] args) throws Exception {
    final BGDClient bgdClient = BGDClient.fromCommandLine(args);
    final Configuration runtimeConfiguration = UnsafeHDInsightRuntimeConfiguration.fromEnvironment();
    final LauncherStatus result = bgdClient.run(runtimeConfiguration, "BGDHDI");
    System.out.println("Result: " + result.toString());
  }
}
