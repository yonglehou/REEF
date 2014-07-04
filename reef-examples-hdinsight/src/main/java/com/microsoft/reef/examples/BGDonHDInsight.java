package com.microsoft.reef.examples;

import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.examples.nggroup.bgd.BGDREEF;
import com.microsoft.reef.runtime.hdinsight.client.UnsafeHDInsightRuntimeConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.InjectionException;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Runs BGD on HDInsight
 */
public class BGDonHDInsight {
  private static final Logger LOG = Logger.getLogger(BGDonHDInsight.class.getName());

  public static void main(final String[] args) throws IOException, InjectionException {
    final Configuration runtimeConfiguration = UnsafeHDInsightRuntimeConfiguration.fromEnvironment();
    final LauncherStatus state = BGDREEF.runBGDReef(runtimeConfiguration, args);
    LOG.log(Level.INFO, "REEF job completed: {0}", state);
  }
}
