/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.runtime.local.driver;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.driver.api.ResourceLaunchHandler;
import com.microsoft.reef.runtime.common.driver.api.ResourceReleaseHandler;
import com.microsoft.reef.runtime.common.driver.api.ResourceRequestHandler;
import com.microsoft.reef.runtime.common.driver.api.RuntimeParameters;
import com.microsoft.reef.runtime.common.launch.CLRLaunchCommandBuilder;
import com.microsoft.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import com.microsoft.reef.runtime.common.launch.LaunchCommandBuilder;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.ConfigurationSerializer;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A resource manager that uses threads to execute containers.
 */
@Private
@DriverSide
@Unit
public final class ResourceManager {

  private final static Logger LOG = Logger.getLogger(ResourceManager.class.getName());
  private static final String EVALUATOR_CONFIGURATION_NAME = "evaluator.conf";

  private final EventHandler<DriverRuntimeProtocol.ResourceAllocationProto> allocationHandler;
  private final ResourceRequestQueue requestQueue = new ResourceRequestQueue();
  private final ContainerManager theContainers;
  private final EventHandler<DriverRuntimeProtocol.RuntimeStatusProto> runtimeStatusHandlerEventHandler;
  private final int defaultMemorySize;
  private final ConfigurationSerializer configurationSerializer;

  private final RemoteManager remoteManager;

  /**
   * Libraries to be added to all evaluators.
   */
  private final List<String> globalLibraries;

  private final Set<File> globalFilesAndLibraries;

  @Inject
  ResourceManager(final ContainerManager cm,
                  final @Parameter(RuntimeParameters.ResourceAllocationHandler.class) EventHandler<DriverRuntimeProtocol.ResourceAllocationProto> allocationHandler,
                  final @Parameter(RuntimeParameters.RuntimeStatusHandler.class) EventHandler<DriverRuntimeProtocol.RuntimeStatusProto> runtimeStatusHandlerEventHandler,
                  final @Parameter(LocalDriverConfiguration.GlobalLibraries.class) Set<String> globalLibraries,
                  final @Parameter(LocalDriverConfiguration.GlobalFiles.class) Set<String> globalFiles,
                  final @Parameter(LocalRuntimeConfiguration.DefaultMemorySize.class) int defaultMemorySize,
                  final ConfigurationSerializer configurationSerializer,
                  final RemoteManager remoteManager) {
    this.theContainers = cm;
    this.allocationHandler = allocationHandler;
    this.runtimeStatusHandlerEventHandler = runtimeStatusHandlerEventHandler;
    this.configurationSerializer = configurationSerializer;
    this.remoteManager = remoteManager;
    this.defaultMemorySize = defaultMemorySize;
    this.globalLibraries = new ArrayList<>(globalLibraries);
    Collections.sort(this.globalLibraries);

    this.globalFilesAndLibraries = new HashSet<>(globalFiles.size() + globalLibraries.size());

    for (final String fileName : globalFiles) {
      this.globalFilesAndLibraries.add(new File(fileName));
    }
    for (final String fileName : this.globalLibraries) {
      this.globalFilesAndLibraries.add(new File(fileName));
    }


    LOG.log(Level.INFO, "Instantiated 'ResourceManager'");
  }

  /**
   * Receives a resource request.
   * <p/>
   * If the request can be met, it will also be satisfied immediately.
   *
   * @param resourceRequest the resource request to be handled.
   */
  final void onNext(final DriverRuntimeProtocol.ResourceRequestProto resourceRequest) {
    synchronized (this.theContainers) {
      this.requestQueue.add(new ResourceRequest(resourceRequest));
      this.checkQ();
    }
  }

  /**
   * Receives and processes a resource release request.
   *
   * @param releaseRequest the release request to be processed
   */
  final void onNext(final DriverRuntimeProtocol.ResourceReleaseProto releaseRequest) {
    synchronized (this.theContainers) {
      LOG.log(Level.FINEST, "Release container " + releaseRequest.getIdentifier());
      this.theContainers.release(releaseRequest.getIdentifier());
      this.checkQ();
    }
  }

  /**
   * Processes a resource launch request.
   *
   * @param launchRequest the launch request to be processed.
   */
  final void onNext(final DriverRuntimeProtocol.ResourceLaunchProto launchRequest) {
    synchronized (this.theContainers) {
      final Container c = this.theContainers.get(launchRequest.getIdentifier());

      // Add the global files and libraries.
      c.addFiles(this.globalFilesAndLibraries);
      c.addFiles(getLocalFiles(launchRequest));

      // Assemble the classpath.
      final List<String> classPath = this.assembleClasspath(getLocalLibraries(launchRequest));

      // Make the configuration file of the evaluator.
      final File evaluatorConfigurationFile = new File(c.getFolder(), EVALUATOR_CONFIGURATION_NAME);

      try {
        this.configurationSerializer.toFile(this.configurationSerializer.fromString(launchRequest.getEvaluatorConf()), evaluatorConfigurationFile);
      } catch (final IOException | BindException e) {
        throw new RuntimeException("Unable to write configuration.", e);
      }

      // Assemble the command line
      final LaunchCommandBuilder commandBuilder;
      switch (launchRequest.getType()) {
        case JVM:
          commandBuilder = new JavaLaunchCommandBuilder().setClassPath(classPath);
          break;
        case CLR:
          commandBuilder = new CLRLaunchCommandBuilder();
          break;
        default:
          throw new IllegalArgumentException("Unsupported container type: " + launchRequest.getType());
      }

      final List<String> command = commandBuilder
          .setErrorHandlerRID(this.remoteManager.getMyIdentifier())
          .setLaunchID(c.getNodeID())
          .setConfigurationFileName(evaluatorConfigurationFile.getName())
          .setMemory(c.getMemory())
          .build();

      LOG.log(Level.FINEST, "Launching container " + c);
      c.run(command);
    }
  }

  /**
   * Checks the allocation queue for new allocations and if there are any
   * satisfies them.
   */
  private void checkQ() {
    if (this.theContainers.hasContainerAvailable() && this.requestQueue.hasOutStandingRequests()) {
      // Record the satisfaction of one request and get its details.
      final DriverRuntimeProtocol.ResourceRequestProto requestProto = this.requestQueue.satisfyOne();

      // Allocate a Container
      final Container container;
      if (requestProto.hasMemorySize()) {
        container = this.theContainers.allocateOne(requestProto.getMemorySize());
      } else {
        container = this.theContainers.allocateOne(this.defaultMemorySize);
      }


      // Tell the receivers about it
      final DriverRuntimeProtocol.ResourceAllocationProto alloc =
          DriverRuntimeProtocol.ResourceAllocationProto.newBuilder()
              .setIdentifier(container.getContainerID())
              .setNodeId(container.getNodeID())
              .setResourceMemory(container.getMemory())
              .build();

      LOG.log(Level.FINEST, "Allocating container " + container);
      this.allocationHandler.onNext(alloc);

      // update REEF
      this.sendRuntimeStatus();
      // Check whether we can satisfy another one.
      this.checkQ();
    } else {
      this.sendRuntimeStatus();
    }
  }

  private void sendRuntimeStatus() {
    final DriverRuntimeProtocol.RuntimeStatusProto.Builder b = DriverRuntimeProtocol.RuntimeStatusProto.newBuilder()
        .setName("LOCAL")
        .setState(ReefServiceProtos.State.RUNNING)
        .setOutstandingContainerRequests(this.requestQueue.getNumberOfOutstandingRequests())
        .addAllContainerAllocation(this.theContainers.getAllocatedContainerIDs());
    final DriverRuntimeProtocol.RuntimeStatusProto msg = b.build();
    final String logMessage = "Outstanding Container Requests: " + msg.getOutstandingContainerRequests() + ", AllocatedContainers: " + msg.getContainerAllocationCount();
    LOG.log(Level.FINEST, logMessage);
    this.runtimeStatusHandlerEventHandler.onNext(msg);
  }

  /**
   * Assembles the class path: sorts localLibraries and adds the globalLibraries
   *
   * @param localLibraries a list of file names to assemble to a classpath.
   * @return a classpath list.
   */
  private List<String> assembleClasspath(final List<String> localLibraries) {
    Collections.sort(localLibraries);
    final ArrayList<String> classPathList = new ArrayList<>(this.globalLibraries.size() + localLibraries.size());
    classPathList.addAll(localLibraries);
    classPathList.addAll(this.globalLibraries);
    return classPathList;
  }

  /**
   * Extracts the libraries out of the launchRequest.
   *
   * @param launchRequest the ResourceLaunchProto to parse
   * @return a list of libraries set in the given ResourceLaunchProto
   */
  private static List<String> getLocalLibraries(final DriverRuntimeProtocol.ResourceLaunchProto launchRequest) {
    final List<String> localLibraries = new ArrayList<>();  // Libraries local to this evaluator
    for (final ReefServiceProtos.FileResourceProto frp : launchRequest.getFileList()) {
      if (frp.getType() == ReefServiceProtos.FileType.LIB) {
        localLibraries.add(frp.getName());
      }
    }
    return localLibraries;
  }

  /**
   * Extracts the files out of the launchRequest.
   *
   * @param launchRequest the ResourceLaunchProto to parse
   * @return a list of files set in the given ResourceLaunchProto
   */
  private static List<File> getLocalFiles(final DriverRuntimeProtocol.ResourceLaunchProto launchRequest) {
    final List<File> files = new ArrayList<>();  // Libraries local to this evaluator
    for (final ReefServiceProtos.FileResourceProto frp : launchRequest.getFileList()) {
      files.add(new File(frp.getPath()).getAbsoluteFile());
    }
    return files;
  }

  /**
   * Takes resource launch events and patches them through to the ResourceManager.
   */
  @Private
  @DriverSide
  public class LocalResourceLaunchHandler implements ResourceLaunchHandler {
    @Override
    public void onNext(final DriverRuntimeProtocol.ResourceLaunchProto t) {
      ResourceManager.this.onNext(t);
    }
  }

  /**
   * Takes Resource Release requests and patches them through to the resource
   * manager.
   */
  @Private
  @DriverSide
  public class LocalResourceReleaseHandler implements ResourceReleaseHandler {
    @Override
    public void onNext(final DriverRuntimeProtocol.ResourceReleaseProto t) {
      ResourceManager.this.onNext(t);
    }
  }


  /**
   * Takes resource requests and patches them through to the ResourceManager
   */
  @Private
  @DriverSide
  public class LocalResourceRequestHandler implements ResourceRequestHandler {
    @Override
    public void onNext(final DriverRuntimeProtocol.ResourceRequestProto t) {
      ResourceManager.this.onNext(t);
    }
  }
}
