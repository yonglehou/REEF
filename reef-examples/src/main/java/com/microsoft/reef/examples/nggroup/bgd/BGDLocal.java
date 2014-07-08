package com.microsoft.reef.examples.nggroup.bgd;

import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.tang.Configuration;

/**
 * Runs BGD on the local runtime.
 */
public class BGDLocal {
  private static final String NUM_LOCAL_THREADS = "20";

  public static void main(final String[] args) throws Exception {
    final BGDClient bgdClient = BGDClient.fromCommandLine(args);
    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUM_LOCAL_THREADS)
        .build();
    final LauncherStatus result = bgdClient.run(runtimeConfiguration, "BGDLocal");
    System.out.println("Result: " + result.toString());
  }
}
