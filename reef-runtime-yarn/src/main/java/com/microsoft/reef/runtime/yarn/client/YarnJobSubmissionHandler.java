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
package com.microsoft.reef.runtime.yarn.client;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.io.SystemTempFileCreator;
import com.microsoft.reef.io.TempFileCreator;
import com.microsoft.reef.proto.ClientRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.client.api.JobSubmissionHandler;
import com.microsoft.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import com.microsoft.reef.runtime.yarn.driver.YarnMasterConfiguration;
import com.microsoft.reef.runtime.yarn.util.YarnUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.ConfigurationSerializer;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import javax.inject.Inject;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@Private
@ClientSide
final class YarnJobSubmissionHandler implements JobSubmissionHandler {

  private static final Logger LOG = Logger.getLogger(YarnJobSubmissionHandler.class.getName());
  private final YarnConfiguration yarnConfiguration;
  private final YarnClient yarnClient;
  private final ConfigurationSerializer configurationSerializer;
  private final TempFileCreator tempFileCreator = new SystemTempFileCreator();

  @Inject
  YarnJobSubmissionHandler(final YarnConfiguration yarnConfiguration,
                           final ConfigurationSerializer configurationSerializer) {
    this.yarnConfiguration = yarnConfiguration;
    this.configurationSerializer = configurationSerializer;
    this.yarnClient = YarnClient.createYarnClient();
    this.yarnClient.init(this.yarnConfiguration);
    this.yarnClient.start();
  }

  @Override
  public void close() {
    this.yarnClient.stop();
  }

  @Override
  public void onNext(final ClientRuntimeProtocol.JobSubmissionProto jobSubmissionProto) {
    try {
      // Get a new application id
      final YarnClientApplication yarnClientApplication = yarnClient.createApplication();
      final GetNewApplicationResponse applicationResponse = yarnClientApplication.getNewApplicationResponse();

      final ApplicationSubmissionContext applicationSubmissionContext = yarnClientApplication.getApplicationSubmissionContext();
      final ApplicationId applicationId = applicationSubmissionContext.getApplicationId();

      LOG.log(Level.FINEST, "YARN Application ID: {0}", applicationId);

      // set the application name
      final String jobName = "reef-job-" + jobSubmissionProto.getIdentifier();
      applicationSubmissionContext.setApplicationName(jobName);

      final FileSystem fs = FileSystem.get(this.yarnConfiguration);
      final String root_dir = "/tmp/reef-" + jobSubmissionProto.getUserName();
      final Path job_dir = new Path(root_dir, jobName + "/" + applicationId.getId() + "/");
      final Path global_dir = new Path(job_dir, YarnMasterConfiguration.GLOBAL_FILE_DIRECTORY);

      ///////////////////////////////////////////////////////////////////////
      // FILE RESOURCES
      final Map<String, LocalResource> localResources = new HashMap<>();

      final File yarnConfigurationFile = this.tempFileCreator.createTempFile("yarn", ".conf");
      final FileOutputStream yarnConfigurationFOS = new FileOutputStream(yarnConfigurationFile);
      this.yarnConfiguration.writeXml(yarnConfigurationFOS);
      yarnConfigurationFOS.close();

      LOG.log(Level.FINEST, "Upload tmp yarn configuration file {0}", yarnConfigurationFile.toURI());
      localResources.put(yarnConfigurationFile.getName(),
          YarnUtils.getLocalResource(fs, new Path(yarnConfigurationFile.toURI()), new Path(global_dir, yarnConfigurationFile.getName())));

      final StringBuilder globalClassPath = YarnUtils.getClassPathBuilder(this.yarnConfiguration);

      for (final ReefServiceProtos.FileResourceProto file : jobSubmissionProto.getGlobalFileList()) {
        final Path src = new Path(file.getPath());
        final Path dst = new Path(global_dir, file.getName());
        switch (file.getType()) {
          case PLAIN:
            LOG.log(Level.FINEST, "GLOBAL FILE RESOURCE: upload {0} to {1}",
                new Object[]{src, dst});
            localResources.put(file.getName(), YarnUtils.getLocalResource(fs, src, dst));
            break;
          case LIB:
            globalClassPath.append(File.pathSeparatorChar + file.getName());
            LOG.log(Level.FINEST, "GLOBAL LIB FILE RESOURCE: upload {0} to {1}",
                new Object[]{src, dst});
            localResources.put(file.getName(), YarnUtils.getLocalResource(fs, src, dst));
            break;
          case ARCHIVE:
            localResources.put(file.getName(), YarnUtils.getLocalResource(fs, src, dst));
            break;
        }
      }

      final StringBuilder localClassPath = new StringBuilder();

      for (final ReefServiceProtos.FileResourceProto file : jobSubmissionProto.getLocalFileList()) {
        final Path src = new Path(file.getPath());
        final Path dst = new Path(job_dir, file.getName());
        switch (file.getType()) {
          case PLAIN:
            LOG.log(Level.FINEST, "LOCAL FILE RESOURCE: upload {0} to {1}",
                new Object[]{src, dst});
            localResources.put(file.getName(), YarnUtils.getLocalResource(fs, src, dst));
            break;
          case LIB:
            localClassPath.append(File.pathSeparatorChar + file.getName());
            LOG.log(Level.FINEST, "LOCAL LIB FILE RESOURCE: upload {0} to {1}",
                new Object[]{src, dst});
            localResources.put(file.getName(), YarnUtils.getLocalResource(fs, src, dst));
            break;
          case ARCHIVE:
            localResources.put(file.getName(), YarnUtils.getLocalResource(fs, src, dst));
            break;
        }
      }

      // RUNTIME CONFIGURATION FILE
      final Configuration masterConfiguration = new YarnMasterConfiguration()
          .setGlobalFileClassPath(globalClassPath.toString())
          .setJobSubmissionDirectory(job_dir.toString())
          .setYarnConfigurationFile(yarnConfigurationFile.getName())
          .setJobIdentifier(jobSubmissionProto.getIdentifier())
          .setClientRemoteIdentifier(jobSubmissionProto.getRemoteId())
          .addClientConfiguration(this.configurationSerializer.fromString(jobSubmissionProto.getConfiguration()))
          .build();
      final File masterConfigurationFile = this.tempFileCreator.createTempFile("driver", ".conf");
      this.configurationSerializer.toFile(masterConfiguration, masterConfigurationFile);

      localResources.put(masterConfigurationFile.getName(),
          YarnUtils.getLocalResource(fs, new Path(masterConfigurationFile.toURI()), new Path(job_dir, masterConfigurationFile.getName())));

      ////////////////////////////////////////////////////////////////////////////

      // SET MEMORY RESOURCE
      final int amMemory;
      final int maxMemory = applicationResponse.getMaximumResourceCapability().getMemory();
      final int requestedMemory = jobSubmissionProto.getDriverMemory();
      if (requestedMemory <= maxMemory) {
        amMemory = requestedMemory;
      } else {
        LOG.log(Level.WARNING, "Requested {0}MB of memory for the driver. The max on this YARN installation is {1}. Using {1} as the memory for the driver.",
            new Object[]{requestedMemory, maxMemory});
        amMemory = maxMemory;
      }
      final Resource capability = Records.newRecord(Resource.class);
      capability.setMemory(amMemory);
      applicationSubmissionContext.setResource(capability);

      final String classPath = "".equals(localClassPath.toString()) ?
          globalClassPath.toString() : localClassPath.toString() + File.pathSeparatorChar + globalClassPath.toString();

      // SET EXEC COMMAND
      final List<String> launchCommandList = new JavaLaunchCommandBuilder()
          .setErrorHandlerRID(jobSubmissionProto.getRemoteId())
          .setLaunchID(jobSubmissionProto.getIdentifier())
          .setConfigurationFileName(masterConfigurationFile.getName())
          .setClassPath(classPath)
          .setMemory(amMemory)
          .setStandardOut(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/driver.stdout")
          .setStandardErr(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/driver.stderr")
          .build();
      final String launchCommand = StringUtils.join(launchCommandList, ' ');
      LOG.log(Level.FINEST, "LAUNCH COMMAND: {0}", launchCommand);

      final ContainerLaunchContext containerContext = YarnUtils.getContainerLaunchContext(launchCommand, localResources);
      applicationSubmissionContext.setAMContainerSpec(containerContext);

      final Priority pri = Records.newRecord(Priority.class);
      pri.setPriority(jobSubmissionProto.hasPriority() ? jobSubmissionProto.getPriority() : 0);
      applicationSubmissionContext.setPriority(pri);

      // Set the queue to which this application is to be submitted in the RM
      applicationSubmissionContext.setQueue(jobSubmissionProto.hasQueue() ? jobSubmissionProto.getQueue() : "default");

      LOG.log(Level.INFO, "Submitting REEF Application to YARN. ID: {0}", applicationId);
      this.yarnClient.submitApplication(applicationSubmissionContext);
      // monitorApplication(applicationId);
    } catch (YarnException | IOException | URISyntaxException | BindException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean monitorApplication(final ApplicationId appId)
      throws YarnException, IOException {

    while (true) {

      // Check app status every 1 second.
      try {
        Thread.sleep(1000);
      } catch (final InterruptedException e) {
        LOG.warning("Thread sleep in monitoring loop interrupted");
      }

      // Get application report for the appId we are interested in
      final ApplicationReport report = yarnClient.getApplicationReport(appId);

      LOG.log(Level.INFO,
          "Got application report from ASM for"
              + ", appId={0}"
              + ", clientToAMToken={1}"
              + ", appDiagnostics={2}"
              + ", appMasterHost={3}"
              + ", appQueue={4}"
              + ", appMasterRpcPort={5}"
              + ", appStartTime={6}"
              + ", yarnAppState={7}"
              + ", distributedFinalState={8}"
              + ", appTrackingUrl={9}"
              + ", appUser={10}",
          new Object[]{
              appId.getId(),
              report.getClientToAMToken(),
              report.getDiagnostics(),
              report.getHost(),
              report.getQueue(),
              report.getRpcPort(),
              report.getStartTime(),
              report.getYarnApplicationState(),
              report.getFinalApplicationStatus(),
              report.getTrackingUrl(),
              report.getUser()}
      );

      final YarnApplicationState state = report.getYarnApplicationState();
      final FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          LOG.info("Application has completed successfully. Breaking monitoring loop");
          return true;
        } else {
          LOG.log(Level.INFO, "Application did finished unsuccessfully."
              + " YarnState={0}, DSFinalStatus={1}"
              + ". Breaking monitoring loop", new Object[]{state, dsStatus});
          return false;
        }
      } else if (YarnApplicationState.KILLED == state
          || YarnApplicationState.FAILED == state) {
        LOG.log(Level.INFO, "Application did not finish."
            + " YarnState={0}, DSFinalStatus={1}"
            + ". Breaking monitoring loop", new Object[]{state, dsStatus});
        return false;
      }
    }
  }
}
