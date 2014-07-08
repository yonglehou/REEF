package com.microsoft.reef.examples.nggroup.bgd;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.examples.nggroup.bgd.parameters.*;
import com.microsoft.reef.io.data.loading.api.DataLoadingRequestBuilder;
import com.microsoft.reef.io.network.nggroup.impl.GroupCommService;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.formats.CommandLine;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import javax.inject.Inject;

/**
 * A client to submit BGD Jobs
 */
public class BGDClient {
  private final String input;
  private final int dimensions;
  private final double lambda;
  private final double eps;
  private final int iters;
  private final int numSplits;
  private final int memory;
  private final boolean rampup;
  private final int minParts;

  @Inject
  public BGDClient(final @Parameter(InputDir.class) String input,
                   final @Parameter(Dimensions.class) int dimensions,
                   final @Parameter(Lambda.class) double lambda,
                   final @Parameter(Eps.class) double eps,
                   final @Parameter(Iterations.class) int iters,
                   final @Parameter(NumSplits.class) int numSplits,
                   final @Parameter(EvaluatorMemory.class) int memory,
                   final @Parameter(RampUp.class) boolean rampup,
                   final @Parameter(MinParts.class) int minParts) {
    this.input = input;
    this.dimensions = dimensions;
    this.lambda = lambda;
    this.eps = eps;
    this.iters = iters;
    this.numSplits = numSplits;
    this.memory = memory;
    this.rampup = rampup;
    this.minParts = minParts;
  }

  /**
   * Runs BGD on the given runtime.
   *
   * @param runtimeConfiguration the runtime to run on.
   * @param jobName              the name of the job on the runtime.
   * @return
   * @throws Exception
   */
  public LauncherStatus run(final Configuration runtimeConfiguration, final String jobName) throws Exception {
    final Configuration driverConfiguration = getDriverConfiguration(jobName);
    return DriverLauncher.getLauncher(runtimeConfiguration).run(driverConfiguration);
  }

  /**
   * Runs BGD on the given runtime.
   *
   * @param runtimeConfiguration the runtime to run on.
   * @param jobName              the name of the job on the runtime.
   * @param timeout              the time after which the job will be killed if not completed, in ms
   * @return
   * @throws Exception
   */
  public LauncherStatus run(final Configuration runtimeConfiguration, final String jobName, final int timeout) throws Exception {
    final Configuration driverConfiguration = getDriverConfiguration(jobName);
    return DriverLauncher.getLauncher(runtimeConfiguration).run(driverConfiguration, timeout);
  }

  private final Configuration getDriverConfiguration(final String jobName) {
    final JobConf jobConf = new JobConf();
    jobConf.setInputFormat(TextInputFormat.class);
    TextInputFormat.addInputPath(jobConf, new Path(input));
    final EvaluatorRequest computeRequest = EvaluatorRequest.newBuilder()
        .setNumber(1)
        .setMemory(memory)
        .build();
    final Configuration dataLoadConfiguration = new DataLoadingRequestBuilder()
        .setMemoryMB(memory)
        .setInputFormatClass(TextInputFormat.class)
        .setInputPath(input)
        .setNumberOfDesiredSplits(numSplits)
        .setComputeRequest(computeRequest)
        .setDriverConfigurationModule(EnvironmentUtils
            .addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
            .set(DriverConfiguration.ON_CONTEXT_ACTIVE, BGDDriver.ContextActiveHandler.class)
            .set(DriverConfiguration.ON_CONTEXT_CLOSED, BGDDriver.ContextCloseHandler.class)
            .set(DriverConfiguration.ON_TASK_RUNNING, BGDDriver.TaskRunningHandler.class)
            .set(DriverConfiguration.ON_TASK_FAILED, BGDDriver.TaskFailedHandler.class)
            .set(DriverConfiguration.ON_TASK_COMPLETED, BGDDriver.TaskCompletedHandler.class)
            .set(DriverConfiguration.DRIVER_IDENTIFIER, jobName))
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

  public static final BGDClient fromCommandLine(final String[] args) throws Exception {
    final JavaConfigurationBuilder configurationBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine commandLine = new CommandLine(configurationBuilder);
    commandLine.registerShortNameOfClass(InputDir.class);
    commandLine.registerShortNameOfClass(Dimensions.class);
    commandLine.registerShortNameOfClass(Lambda.class);
    commandLine.registerShortNameOfClass(Eps.class);
    commandLine.registerShortNameOfClass(Iterations.class);
    commandLine.registerShortNameOfClass(NumSplits.class);
    commandLine.registerShortNameOfClass(Timeout.class);
    commandLine.registerShortNameOfClass(EvaluatorMemory.class);
    commandLine.registerShortNameOfClass(RampUp.class);
    commandLine.registerShortNameOfClass(MinParts.class);
    commandLine.processCommandLine(args);
    return Tang.Factory.getTang().newInjector(configurationBuilder.build()).getInstance(BGDClient.class);
  }

}
