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

package com.microsoft.reef.javabridge.generic;

import com.microsoft.reef.client.*;
import com.microsoft.reef.io.network.naming.NameServerConfiguration;
import com.microsoft.reef.javabridge.NativeInterop;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.reef.util.logging.Config;
import com.microsoft.reef.webserver.HttpHandlerConfiguration;
import com.microsoft.reef.webserver.HttpServerReefEventHandler;
import com.microsoft.reef.webserver.ReefEventStateManager;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Configurations;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.AvroConfigurationSerializer;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.wake.EventHandler;
import javax.inject.Inject;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Clr Bridge Client.
 */
@Unit
public class JobClient {

  /**
   * Standard java logger.
   */
  private static final Logger LOG = Logger.getLogger(JobClient.class.getName());

  /**
   * Reference to the REEF framework.
   * This variable is injected automatically in the constructor.
   */
  private final REEF reef;

  /**
   * Job Driver configuration.
   */
  private Configuration driverConfiguration;
  private ConfigurationModule driverConfigModule;

  /**
   * A reference to the running job that allows client to send messages back to the job driver
   */
  private RunningJob runningJob;

  /**
   * Set to false when job driver is done.
   */
  private boolean isBusy = true;

  private int driverMemory;

  private String driverId;

  private String jobSubmissionDirectory = "reefTmp/job_" + System.currentTimeMillis();

  /**
   * Clr Bridge client.
   * Parameters are injected automatically by TANG.
   *
   * @param reef    Reference to the REEF framework.
   */
  @Inject
  JobClient(final REEF reef) throws BindException {
    this.reef = reef;
    this.driverConfigModule =  getDriverConfiguration();
  }

  public static ConfigurationModule getDriverConfiguration() {
    return EnvironmentUtils.addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, JobDriver.AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, JobDriver.FailedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, JobDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_DRIVER_RESTART_CONTEXT_ACTIVE, JobDriver.DriverRestartActiveContextHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_CLOSED, JobDriver.ClosedContextHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_FAILED, JobDriver.FailedContextHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_MESSAGE, JobDriver.ContextMessageHandler.class)
        .set(DriverConfiguration.ON_TASK_MESSAGE, JobDriver.TaskMessageHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, JobDriver.FailedTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, JobDriver.RunningTaskHandler.class)
        .set(DriverConfiguration.ON_DRIVER_RESTART_TASK_RUNNING, JobDriver.DriverRestartRunningTaskHandler.class)
        .set(DriverConfiguration.ON_DRIVER_RESTART_COMPLETED, JobDriver.DriverRestartCompletedHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, JobDriver.CompletedTaskHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STARTED, JobDriver.StartHandler.class)
        .set(DriverConfiguration.ON_DRIVER_RESTARTED, JobDriver.RestartHandler.class)
        .set(DriverConfiguration.ON_TASK_SUSPENDED, JobDriver.SuspendedTaskHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_COMPLETED, JobDriver.CompletedEvaluatorHandler.class);
  }

  public void addCLRFiles( final File folder) throws BindException{
    ConfigurationModule result = this.driverConfigModule;
    for (final File f : folder.listFiles()) {
      if (f.canRead() && f.exists() && f.isFile()) {
        result = result.set(DriverConfiguration.GLOBAL_FILES, f.getAbsolutePath());
      }
    }

    // set the driver memory, id and job submission directory
    this.driverConfigModule  = result
        .set(DriverConfiguration.DRIVER_MEMORY, this.driverMemory)
        .set(DriverConfiguration.DRIVER_IDENTIFIER, this.driverId)
        .set(DriverConfiguration.DRIVER_JOB_SUBMISSION_DIRECTORY, this.jobSubmissionDirectory);


    Path globalLibFile =  Paths.get(NativeInterop.GLOBAL_LIBRARIES_FILENAME);
    if (!Files.exists(globalLibFile))
    {
      LOG.log(Level.FINE, "Cannot find global classpath file at: {0}, assume there is none.", globalLibFile.toAbsolutePath());
    }
    else
    {
      String globalLibString = "";
      try
      {
        globalLibString = new String(Files.readAllBytes(globalLibFile));
      }
      catch(final Exception e)
      {
        LOG.log(Level.WARNING, "Cannot read from {0}, global libraries not added  " + globalLibFile.toAbsolutePath());
      }

      for (final String s : globalLibString.split(","))
      {
        File f = new File(s);
        this.driverConfigModule = this.driverConfigModule.set(DriverConfiguration.GLOBAL_LIBRARIES, f.getPath());
      }
    }

    this.driverConfiguration = Configurations.merge(this.driverConfigModule.build(), getHTTPConfiguration(), getNameServerConfiguration());
  }

  private static Configuration getNameServerConfiguration(){
    return NameServerConfiguration.CONF
        .set(NameServerConfiguration.NAME_SERVICE_PORT, 0)
        .build();
  }

  /**
   * @return the driver-side configuration to be merged into the DriverConfiguration to enable the HTTP server.
   */
  public static Configuration getHTTPConfiguration() {
    Configuration httpHandlerConfiguration = HttpHandlerConfiguration.CONF
        .set(HttpHandlerConfiguration.HTTP_HANDLERS, HttpServerReefEventHandler.class)
        .build();

    Configuration driverConfigurationForHttpServer = DriverServiceConfiguration.CONF
        .set(DriverServiceConfiguration.ON_EVALUATOR_ALLOCATED, ReefEventStateManager.AllocatedEvaluatorStateHandler.class)
        .set(DriverServiceConfiguration.ON_CONTEXT_ACTIVE, ReefEventStateManager.ActiveContextStateHandler.class)
        .set(DriverServiceConfiguration.ON_DRIVER_RESTART_CONTEXT_ACTIVE, ReefEventStateManager.DrivrRestartActiveContextStateHandler.class)
        .set(DriverServiceConfiguration.ON_TASK_RUNNING, ReefEventStateManager.TaskRunningStateHandler.class)
        .set(DriverServiceConfiguration.ON_DRIVER_RESTART_TASK_RUNNING, ReefEventStateManager.DriverRestartTaskRunningStateHandler.class)
        .set(DriverServiceConfiguration.ON_DRIVER_STARTED, ReefEventStateManager.StartStateHandler.class)
        .set(DriverServiceConfiguration.ON_DRIVER_STOP, ReefEventStateManager.StopStateHandler.class)
        .build();
    return Configurations.merge(httpHandlerConfiguration, driverConfigurationForHttpServer);
  }

  /**
   * Launch the job driver.
   *
   * @throws com.microsoft.tang.exceptions.BindException configuration error.
   */
  public void submit(final File clrFolder, final boolean submitDriver, final Configuration clientConfig) {
    try
    {
      addCLRFiles(clrFolder);
    }
    catch(final BindException e)
    {
      LOG.log(Level.FINE, "Failed to bind", e);
    }
    if(submitDriver) {
      this.reef.submit(this.driverConfiguration);
    }else{
      File driverConfig = new File(System.getProperty("user.dir") + "/driver.config");
      try
      {
        new AvroConfigurationSerializer().toFile(Configurations.merge(this.driverConfiguration, clientConfig), driverConfig);
        LOG.log(Level.INFO, "Driver configuration file created at " + driverConfig.getAbsolutePath());
      }catch (final IOException e)
      {
        throw new RuntimeException("Cannot create driver configuration file at " + driverConfig.getAbsolutePath());
      }
    }
  }

  /**
   * Set the driver memory
   */
  public void setDriverInfo(final String identifier, final int memory, final String jobSubmissionDirectory)
  {
    if (identifier == null || identifier.isEmpty()){
      throw new RuntimeException("driver id cannot be null or empty");
    }
    if (memory <= 0){
      throw new RuntimeException("driver memory cannot be negative number: " + memory);
    }
    this.driverMemory = memory;
    this.driverId = identifier;
    if (jobSubmissionDirectory != null && !jobSubmissionDirectory.equals("empty")){
      this.jobSubmissionDirectory = jobSubmissionDirectory;
    }
    else{
      LOG.log(Level.FINE, "No job submission directory provided by CLR user, will use " + this.jobSubmissionDirectory);
    }
  }

  /**
   * Receive notification from the job driver that the job had failed.
   */
  final class FailedJobHandler implements EventHandler<FailedJob> {
      @Override
      public void onNext(final FailedJob job) {
        LOG.log(Level.SEVERE, "Failed job: " + job.getId(), job.getMessage());
        stopAndNotify();
      }
    }

  /**
   * Receive notification from the job driver that the job had completed successfully.
   */
  final class CompletedJobHandler implements EventHandler<CompletedJob> {
    @Override
    public void onNext(final CompletedJob job) {
      LOG.log(Level.INFO, "Completed job: {0}", job.getId());
      stopAndNotify();
    }
  }

  /**
   * Receive notification that there was an exception thrown from the job driver.
   */
  final class RuntimeErrorHandler implements EventHandler<FailedRuntime> {
    @Override
    public void onNext(final FailedRuntime error) {
      LOG.log(Level.SEVERE, "Error in job driver: " + error, error.getMessage());
      stopAndNotify();
    }
  }

  final class WakeErrorHandler implements EventHandler<Throwable> {
    @Override
    public void onNext(Throwable error) {
      LOG.log(Level.SEVERE, "Error communicating with job driver, exiting... ", error);
      stopAndNotify();
    }
  }


  /**
   * Notify the process in waitForCompletion() method that the main process has finished.
   */
  private synchronized void stopAndNotify() {
    this.runningJob = null;
    this.isBusy = false;
    this.notify();
  }

  /**
   * Wait for the job driver to complete. This method is called from Launcher.main()
   */
  public void waitForCompletion(final int waitTime) {
    LOG.info("Waiting for the Job Driver to complete: " + waitTime);
    if(waitTime == 0)
    {
      close(0);
      return;
    }
    else if(waitTime < 0)
    {
      waitTillDone();
    }
    long endTime = System.currentTimeMillis() + waitTime * 1000;
    close(endTime);
  }

  public void close(final long endTime)
  {
    while (endTime > System.currentTimeMillis())
    {
      try
      {
        Thread.sleep(1000);
      }
      catch (final InterruptedException e)
      {
        LOG.log(Level.SEVERE, "Thread sleep failed");
      }
    }
    LOG.log(Level.INFO, "Done waiting.");
    this.stopAndNotify();
    reef.close();
  }

  private void waitTillDone()
  {
    while (this.isBusy) {
      try {
        synchronized (this) {
          this.wait();
        }
      } catch (final InterruptedException ex) {
        LOG.log(Level.WARNING, "Waiting for result interrupted.", ex);}
        }
    this.reef.close();
  }
}
