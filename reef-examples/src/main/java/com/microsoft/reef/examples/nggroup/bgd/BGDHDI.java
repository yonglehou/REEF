package com.microsoft.reef.examples.nggroup.bgd;

import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.tang.Configuration;

/**
 * Runs BGD on HDInsight
 */
public class BGDHDI {
  public static void main(final String[] args) throws Exception {
    final BGDClient bgdClient = BGDClient.fromCommandLine(args);
    final Configuration runtimeConfiguration = null;
    final LauncherStatus result = bgdClient.run(runtimeConfiguration, "BGDHDI");
    System.out.println("Result: " + result.toString());
  }
}
