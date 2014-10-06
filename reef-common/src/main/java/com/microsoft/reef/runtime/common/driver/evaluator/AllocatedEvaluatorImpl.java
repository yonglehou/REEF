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
package com.microsoft.reef.runtime.common.driver.evaluator;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.driver.evaluator.EvaluatorType;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.evaluator.EvaluatorConfiguration;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationSerializer;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver-Side representation of an allocated evaluator.
 */
@DriverSide
@Private
final class AllocatedEvaluatorImpl implements AllocatedEvaluator {

  private final static Logger LOG = Logger.getLogger(AllocatedEvaluatorImpl.class.getName());

  private final EvaluatorManager evaluatorManager;
  private final String remoteID;
  private final ConfigurationSerializer configurationSerializer;
  private final String jobIdentifier;

  /**
   * The set of files to be places on the Evaluator.
   */
  private final Collection<File> files = new HashSet<>();
  /**
   * The set of libraries
   */
  private final Collection<File> libraries = new HashSet<>();

  AllocatedEvaluatorImpl(final EvaluatorManager evaluatorManager,
                         final String remoteID,
                         final ConfigurationSerializer configurationSerializer,
                         final String jobIdentifier) {
    this.evaluatorManager = evaluatorManager;
    this.remoteID = remoteID;
    this.configurationSerializer = configurationSerializer;
    this.jobIdentifier = jobIdentifier;
  }

  @Override
  public String getId() {
    return this.evaluatorManager.getId();
  }

  @Override
  public void close() {
    this.evaluatorManager.close();
  }

  @Override
  public void submitTask(final Configuration taskConfiguration) {
    final Configuration contextConfiguration = ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, "RootContext_" + this.getId())
        .build();
    this.submitContextAndTask(contextConfiguration, taskConfiguration);

  }

  @Override
  public EvaluatorDescriptor getEvaluatorDescriptor() {
    return this.evaluatorManager.getEvaluatorDescriptor();
  }


  @Override
  public void submitContext(final Configuration contextConfiguration) {
    launch(contextConfiguration, Optional.<Configuration>empty(), Optional.<Configuration>empty());
  }

  @Override
  public void submitContextAndService(final Configuration contextConfiguration,
                                      final Configuration serviceConfiguration) {
    launch(contextConfiguration, Optional.of(serviceConfiguration), Optional.<Configuration>empty());
  }

  @Override
  public void submitContextAndTask(final Configuration contextConfiguration,
                                   final Configuration taskConfiguration) {
    launch(contextConfiguration, Optional.<Configuration>empty(), Optional.of(taskConfiguration));
  }

  @Override
  public void submitContextAndServiceAndTask(final Configuration contextConfiguration,
                                             final Configuration serviceConfiguration,
                                             final Configuration taskConfiguration) {
    launch(contextConfiguration, Optional.of(serviceConfiguration), Optional.of(taskConfiguration));
  }

  @Override
  public void setType(final EvaluatorType type) {
    this.evaluatorManager.setType(type);
  }

  @Override
  public void addFile(final File file) {
    this.files.add(file);
  }

  @Override
  public void addLibrary(final File file) {
    this.libraries.add(file);
  }

  private final void launch(final Configuration contextConfiguration,
                            final Optional<Configuration> serviceConfiguration,
                            final Optional<Configuration> taskConfiguration) {
    try {
      final ConfigurationModule evaluatorConfigurationModule = EvaluatorConfiguration.CONF
          .set(EvaluatorConfiguration.APPLICATION_IDENTIFIER, this.jobIdentifier)
          .set(EvaluatorConfiguration.DRIVER_REMOTE_IDENTIFIER, this.remoteID)
          .set(EvaluatorConfiguration.EVALUATOR_IDENTIFIER, this.getId());

      final String encodedContextConfigurationString = this.configurationSerializer.toString(contextConfiguration);
      // Add the (optional) service configuration
      final ConfigurationModule contextConfigurationModule;
      if (serviceConfiguration.isPresent()) {
        // With service configuration
        final String encodedServiceConfigurationString = this.configurationSerializer.toString(serviceConfiguration.get());
        contextConfigurationModule = evaluatorConfigurationModule
            .set(EvaluatorConfiguration.ROOT_SERVICE_CONFIGURATION, encodedServiceConfigurationString)
            .set(EvaluatorConfiguration.ROOT_CONTEXT_CONFIGURATION, encodedContextConfigurationString);
      } else {
        // No service configuration
        contextConfigurationModule = evaluatorConfigurationModule
            .set(EvaluatorConfiguration.ROOT_CONTEXT_CONFIGURATION, encodedContextConfigurationString);
      }

      // Add the (optional) task configuration
      final Configuration evaluatorConfiguration;
      if (taskConfiguration.isPresent()) {
        final String encodedTaskConfigurationString = this.configurationSerializer.toString(taskConfiguration.get());
        evaluatorConfiguration = contextConfigurationModule
            .set(EvaluatorConfiguration.TASK_CONFIGURATION, encodedTaskConfigurationString).build();
      } else {
        evaluatorConfiguration = contextConfigurationModule.build();
      }

      final DriverRuntimeProtocol.ResourceLaunchProto.Builder rbuilder =
          DriverRuntimeProtocol.ResourceLaunchProto.newBuilder()
              .setIdentifier(this.evaluatorManager.getId())
              .setRemoteId(this.remoteID)
              .setEvaluatorConf(configurationSerializer.toString(evaluatorConfiguration));

      for (final File file : this.files) {
        rbuilder.addFile(ReefServiceProtos.FileResourceProto.newBuilder()
            .setName(file.getName())
            .setPath(file.getPath())
            .setType(ReefServiceProtos.FileType.PLAIN)
            .build());
      }

      for (final File lib : this.libraries) {
        rbuilder.addFile(ReefServiceProtos.FileResourceProto.newBuilder()
            .setName(lib.getName())
            .setPath(lib.getPath().toString())
            .setType(ReefServiceProtos.FileType.LIB)
            .build());
      }

      { // Set the type
        switch (this.evaluatorManager.getEvaluatorDescriptor().getType()) {
          case CLR:
            rbuilder.setType(ReefServiceProtos.ProcessType.CLR);
            break;
          default:
            rbuilder.setType(ReefServiceProtos.ProcessType.JVM);
        }
      }

      this.evaluatorManager.onResourceLaunch(rbuilder.build());

    } catch (final BindException ex) {
      LOG.log(Level.SEVERE, "Bad Evaluator configuration", ex);
      throw new RuntimeException("Bad Evaluator configuration", ex);
    }
  }

  @Override
  public String toString() {
    return "AllocatedEvaluator{ID='" + getId() + "\'}";
  }
}
