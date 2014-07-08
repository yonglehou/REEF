package com.microsoft.reef.examples.nggroup.bgd;

import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.tang.Configuration;

/**
 * Runs BGD on the YARN runtime.
 */
public class BGDYarn {

  public static void main(final String[] args) throws Exception {
    final BGDClient bgdClient = BGDClient.fromCommandLine(args);
    final Configuration runtimeConfiguration = YarnClientConfiguration.CONF.build();
    final LauncherStatus result = bgdClient.run(runtimeConfiguration, "BGDYarn");
    System.out.println("Result: " + result.toString());
  }
}
