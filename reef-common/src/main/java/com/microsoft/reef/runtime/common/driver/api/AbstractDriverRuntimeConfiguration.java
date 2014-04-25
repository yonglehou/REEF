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
package com.microsoft.reef.runtime.common.driver.api;

import com.microsoft.reef.runtime.common.launch.parameters.ErrorHandlerRID;
import com.microsoft.reef.util.Builder;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;

public abstract class AbstractDriverRuntimeConfiguration implements Builder<Configuration> {

  protected JavaConfigurationBuilder builder = Tang.Factory.getTang().newConfigurationBuilder();

  protected AbstractDriverRuntimeConfiguration(
      final Class<? extends ResourceLaunchHandler> resourceLaunchHandlerClass,
      final Class<? extends ResourceReleaseHandler> resourceReleaseHandlerClass,
      final Class<? extends ResourceRequestHandler> resourceRequestHandlerClass) {
    try {
      this.builder.bind(ResourceLaunchHandler.class, resourceLaunchHandlerClass);
      this.builder.bind(ResourceReleaseHandler.class, resourceReleaseHandlerClass);
      this.builder.bind(ResourceRequestHandler.class, resourceRequestHandlerClass);

    } catch (final BindException e) {
      throw new RuntimeException(e);
    }
  }

  public final Configuration build() {
    return this.builder.build();
  }

  @NamedParameter(doc = "The job identifier.")
  public final static class JobIdentifier implements Name<String> {
  }

  @NamedParameter(doc = "The client remote identifier.", default_value = ClientRemoteIdentifier.NONE)
  public final static class ClientRemoteIdentifier implements Name<String> {
    /**
     * Indicates that there is no Client.
     */
    public static final String NONE = ErrorHandlerRID.NONE;
  }

  @NamedParameter(doc = "The evaluator timeout (how long to wait before deciding an evaluator is dead.", default_value = "60000")
  public final static class EvaluatorTimeout implements Name<Long> {
  }

  /**
   * This parameter denotes that the driver process should actually be
   * started in a separate process with the given amount of JVM memory.
   */
  @NamedParameter(doc = "The driver process memory.", default_value = "512")
  public final static class DriverProcessMemory implements Name<Integer> {
  }

  public final AbstractDriverRuntimeConfiguration addClientConfiguration(final Configuration conf) {
    try {
      this.builder.addConfiguration(conf);
      return this;
    } catch (final BindException e) {
      throw new RuntimeException(e);
    }
  }

  public final AbstractDriverRuntimeConfiguration setJobIdentifier(final String id) {
    try {
      this.builder.bindNamedParameter(JobIdentifier.class, id.toString());
      return this;
    } catch (final BindException e) {
      throw new RuntimeException(e);
    }
  }

  public final AbstractDriverRuntimeConfiguration setClientRemoteIdentifier(final String rid) {
    try {
      this.builder.bindNamedParameter(ClientRemoteIdentifier.class, rid.toString());
      return this;
    } catch (final BindException e) {
      throw new RuntimeException(e);
    }
  }

  public final AbstractDriverRuntimeConfiguration setDriverProcessMemory(final int memory) {
    try {
      this.builder.bindNamedParameter(DriverProcessMemory.class, Integer.toString(memory));
      return this;
    } catch (final BindException e) {
      throw new RuntimeException(e);
    }
  }

  public final AbstractDriverRuntimeConfiguration setEvaluatorTimeout(final long value) {
    try {
      this.builder.bindNamedParameter(EvaluatorTimeout.class, Long.toString(value));
      return this;
    } catch (final BindException e) {
      throw new RuntimeException(e);
    }
  }
}
