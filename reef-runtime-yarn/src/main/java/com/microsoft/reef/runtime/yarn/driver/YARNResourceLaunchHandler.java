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
package com.microsoft.reef.runtime.yarn.driver;

import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.runtime.common.driver.api.ResourceLaunchHandler;
import com.microsoft.reef.runtime.common.files.REEFClasspath;
import com.microsoft.reef.runtime.common.files.REEFFileNames;
import com.microsoft.reef.runtime.common.files.YarnClasspath;
import com.microsoft.reef.runtime.common.launch.CLRLaunchCommandBuilder;
import com.microsoft.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import com.microsoft.reef.runtime.common.launch.LaunchCommandBuilder;
import com.microsoft.reef.runtime.common.parameters.JVMHeapSlack;
import com.microsoft.reef.runtime.yarn.util.YarnTypes;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.tang.annotations.Parameter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Resource launch handler for YARN.
 */
public final class YARNResourceLaunchHandler implements ResourceLaunchHandler {

  private static final Logger LOG = Logger.getLogger(YARNResourceLaunchHandler.class.getName());

  private final Containers containers;
  private final InjectionFuture<YarnContainerManager> yarnContainerManager;
  private final EvaluatorSetupHelper evaluatorSetupHelper;
  private final REEFFileNames filenames;
  private final REEFClasspath classpath;
  private final double jvmHeapFactor;

  @Inject
  YARNResourceLaunchHandler(final Containers containers,
                            final InjectionFuture<YarnContainerManager> yarnContainerManager,
                            final EvaluatorSetupHelper evaluatorSetupHelper,
                            final REEFFileNames filenames,
                            final YarnClasspath classpath,
                            final @Parameter(JVMHeapSlack.class) double jvmHeapSlack) {
    this.jvmHeapFactor = 1.0 - jvmHeapSlack;
    LOG.log(Level.FINEST, "Instantiating 'YARNResourceLaunchHandler'");
    this.containers = containers;
    this.yarnContainerManager = yarnContainerManager;
    this.evaluatorSetupHelper = evaluatorSetupHelper;
    this.filenames = filenames;
    this.classpath = classpath;
    LOG.log(Level.FINE, "Instantiated 'YARNResourceLaunchHandler'");
  }

  @Override
  public void onNext(final DriverRuntimeProtocol.ResourceLaunchProto resourceLaunchProto) {
    try {

      final String containerId = resourceLaunchProto.getIdentifier();
      LOG.log(Level.FINEST, "TIME: Start ResourceLaunchProto {0}", containerId);
      final Container container = this.containers.get(containerId);
      LOG.log(Level.FINEST, "Setting up container launch container for id={0}", container.getId());
      final Map<String, LocalResource> localResources =
          this.evaluatorSetupHelper.getResources(resourceLaunchProto);

      final LaunchCommandBuilder commandBuilder;
      switch (resourceLaunchProto.getType()) {
        case JVM:
          commandBuilder = new JavaLaunchCommandBuilder()
              .setClassPath(this.classpath.getClasspath());
          break;
        case CLR:
          commandBuilder = new CLRLaunchCommandBuilder();
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported container type: " + resourceLaunchProto.getType());
      }

      final List<String> command = commandBuilder
          .setErrorHandlerRID(resourceLaunchProto.getRemoteId())
          .setLaunchID(resourceLaunchProto.getIdentifier())
          .setConfigurationFileName(this.filenames.getEvaluatorConfigurationPath())
          .setMemory((int) (this.jvmHeapFactor * container.getResource().getMemory()))
          .setStandardErr(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + this.filenames.getEvaluatorStderrFileName())
          .setStandardOut(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + this.filenames.getEvaluatorStdoutFileName())
          .build();

      if (LOG.isLoggable(Level.FINEST)) {
        LOG.log(Level.FINEST,
            "TIME: Run ResourceLaunchProto {0} command: `{1}` with resources: `{2}`",
            new Object[]{containerId, StringUtils.join(command, ' '), localResources});
      }

      final ContainerLaunchContext ctx = YarnTypes.getContainerLaunchContext(command, localResources);
      this.yarnContainerManager.get().submit(container, ctx);

      LOG.log(Level.FINEST, "TIME: End ResourceLaunchProto {0}", containerId);

    } catch (final Throwable e) {
      LOG.log(Level.WARNING, "Error handling resource launch message: " + resourceLaunchProto, e);
      throw new RuntimeException(e);
    }
  }
}
