/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.examples.nggroup.bgd;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.examples.nggroup.bgd.parameters.*;
import com.microsoft.reef.io.data.loading.api.DataLoadingRequestBuilder;
import com.microsoft.reef.io.network.nggroup.impl.GroupCommService;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.AvroConfigurationSerializer;
import com.microsoft.tang.formats.CommandLine;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
@ClientSide
public class BGDREEF {
  private static final Logger LOG = Logger.getLogger(BGDREEF.class.getName());

  private static final String NUM_LOCAL_THREADS = "20";

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "Whether or not to run on the local runtime", short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  @NamedParameter(short_name = "input")
  public static final class InputDir implements Name<String> {
  }

  @NamedParameter(short_name = "splits", default_value = "5")
  public static final class NumSplits implements Name<Integer> {
  }

  @NamedParameter(short_name = "timeout", default_value = "2")
  public static final class Timeout implements Name<Integer> {
  }

  @NamedParameter(short_name = "memory", default_value = "1024")
  public static final class Memory implements Name<Integer> {
  }

  private static boolean local;
  private static String input;
  private static int dimensions;
  private static double lambda;
  private static double eps;
  private static int iters;
  private static int numSplits;
  private static int timeout;
  private static int memory;
  private static boolean rampup;
  private static int minParts;


  private static Configuration parseCommandLine(final String[] aArgs) {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    try {
      final CommandLine cl = new CommandLine(cb);
      cl.registerShortNameOfClass(Local.class);
      cl.registerShortNameOfClass(BGDREEF.InputDir.class);
      cl.registerShortNameOfClass(Dimensions.class);
      cl.registerShortNameOfClass(Lambda.class);
      cl.registerShortNameOfClass(Eps.class);
      cl.registerShortNameOfClass(Iterations.class);
      cl.registerShortNameOfClass(NumSplits.class);
      cl.registerShortNameOfClass(Timeout.class);
      cl.registerShortNameOfClass(Memory.class);
      cl.registerShortNameOfClass(RampUp.class);
      cl.registerShortNameOfClass(MinParts.class);
      cl.processCommandLine(aArgs);
    } catch (final BindException | IOException ex) {
      final String msg = "Unable to parse command line";
      LOG.log(Level.SEVERE, msg, ex);
      throw new RuntimeException(msg, ex);
    }
    return cb.build();
  }

  /**
   * copy the parameters from the command line required for the Client configuration
   */
  private static void storeCommandLineArgs(final Configuration commandLineConf)
      throws InjectionException, BindException {
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    local = injector.getNamedInstance(Local.class);
    input = injector.getNamedInstance(BGDREEF.InputDir.class);
    dimensions = injector.getNamedInstance(Dimensions.class);
    lambda = injector.getNamedInstance(Lambda.class);
    eps = injector.getNamedInstance(Eps.class);
    iters = injector.getNamedInstance(Iterations.class);
    numSplits = injector.getNamedInstance(NumSplits.class);
    timeout = injector.getNamedInstance(Timeout.class);
    memory = injector.getNamedInstance(Memory.class);
    rampup = injector.getNamedInstance(RampUp.class);
    minParts = injector.getNamedInstance(MinParts.class);
  }

  /**
   * @param commandLineConf Command line arguments, as passed into main().
   * @return (immutable) TANG Configuration object.
   * @throws BindException      if configuration injector fails.
   * @throws InjectionException if the Local.class parameter is not injected.
   */
  private static Configuration getRunTimeConfiguration() throws BindException {
    final Configuration runtimeConfiguration;
    if (local) {
      LOG.log(Level.INFO, "Running BGD using nggroup API on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUM_LOCAL_THREADS)
          .build();
    } else {
      LOG.log(Level.INFO, "Running BGD using nggroup API on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    }
    return runtimeConfiguration;
  }


  private static Configuration getDriverConfiguration() {
    final JobConf jobConf = new JobConf();
    jobConf.setInputFormat(TextInputFormat.class);
    TextInputFormat.addInputPath(jobConf, new Path(input));
    final EvaluatorRequest computeRequest = EvaluatorRequest.newBuilder()
        .setNumber(1)
        .setMemory(memory)
        .build();
    final Configuration dataLoadConfiguration = new DataLoadingRequestBuilder()
        .setMemoryMB(memory)
        .setJobConf(jobConf)
        .setNumberOfDesiredSplits(numSplits)
        .setComputeRequest(computeRequest)
        .setDriverConfigurationModule(EnvironmentUtils
            .addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
            .set(DriverConfiguration.ON_CONTEXT_ACTIVE, BGDDriver.ContextActiveHandler.class)
            .set(DriverConfiguration.ON_CONTEXT_CLOSED, BGDDriver.ContextCloseHandler.class)
            .set(DriverConfiguration.ON_TASK_RUNNING, BGDDriver.TaskRunningHandler.class)
            .set(DriverConfiguration.ON_TASK_FAILED, BGDDriver.TaskFailedHandler.class)
            .set(DriverConfiguration.ON_TASK_COMPLETED, BGDDriver.TaskCompletedHandler.class)
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "BGDDriver"))
        .build();

    final Configuration groupCommServConfiguration = GroupCommService.getConfiguration();

    final Configuration mergedDriverConfiguration = Tang.Factory.getTang()
        .newConfigurationBuilder(groupCommServConfiguration, dataLoadConfiguration)
        .bindNamedParameter(Dimensions.class, Integer.toString(dimensions))
        .bindNamedParameter(Lambda.class, Double.toString(lambda))
        .bindNamedParameter(Eps.class, Double.toString(eps))
        .bindNamedParameter(Iterations.class, Integer.toString(iters))
        .bindNamedParameter(RampUp.class, Boolean.toString(rampup))
        .bindNamedParameter(MinParts.class, Integer.toString(minParts))
        .build();
    return mergedDriverConfiguration;
  }

  public static LauncherStatus runBGDReef(final Configuration runtimeConfiguration, final int jobTimeout)
      throws BindException, InjectionException {
    final Configuration driverConfiguration = getDriverConfiguration();
    LOG.log(Level.FINE, new AvroConfigurationSerializer().toString(driverConfiguration));
    return DriverLauncher.getLauncher(runtimeConfiguration).run(driverConfiguration, jobTimeout);
  }

  public static LauncherStatus runBGDReef(final Configuration runtimeConfiguration)
      throws BindException, InjectionException {
    final Configuration driverConfiguration = getDriverConfiguration();
    LOG.log(Level.FINE, new AvroConfigurationSerializer().toString(driverConfiguration));
    return DriverLauncher.getLauncher(runtimeConfiguration).run(driverConfiguration);
  }

  public static LauncherStatus runBGDReef(final Configuration runtimeConfiguration, final String[] args)
      throws InjectionException {
    final Configuration commandLineConf = parseCommandLine(args);
    storeCommandLineArgs(commandLineConf);
    final LauncherStatus state = runBGDReef(runtimeConfiguration);
    return state;
  }

  /**
   * @param args
   * @throws BindException
   * @throws InjectionException
   */
  public static void main(final String[] args) throws InjectionException, BindException {
    final Configuration commandLineConf = parseCommandLine(args);
    storeCommandLineArgs(commandLineConf);
    final Configuration runtimeConfiguration = getRunTimeConfiguration();
    final LauncherStatus state = runBGDReef(runtimeConfiguration, timeout * 60 * 1000);
    LOG.log(Level.INFO, "REEF job completed: {0}", state);
  }
}
