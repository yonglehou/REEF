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
package com.microsoft.reef.runtime.common.evaluator;

import com.microsoft.reef.runtime.common.evaluator.parameters.*;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.OptionalParameter;
import com.microsoft.tang.formats.RequiredParameter;
import com.microsoft.wake.time.Clock;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The runtime configuration of an evaluator.
 */
public final class EvaluatorConfiguration extends ConfigurationModuleBuilder {
  private final static class ExecutorServiceConstructor implements ExternalConstructor<ExecutorService> {

    @Inject
    ExecutorServiceConstructor() {
    }

    @Override
    public ExecutorService newInstance() {
      return Executors.newCachedThreadPool();
    }
  }

  public static final RequiredParameter<String> DRIVER_REMOTE_IDENTIFIER = new RequiredParameter<>();
  public static final RequiredParameter<String> EVALUATOR_IDENTIFIER = new RequiredParameter<>();
  public static final RequiredParameter<String> ROOT_CONTEXT_CONFIGURATION = new RequiredParameter<>();
  public static final OptionalParameter<String> ROOT_SERVICE_CONFIGURATION = new OptionalParameter<>();
  public static final OptionalParameter<String> TASK_CONFIGURATION = new OptionalParameter<>();
  public static final OptionalParameter<Integer> HEARTBEAT_PERIOD = new OptionalParameter<>();
  public static final OptionalParameter<String> APPLICATION_IDENTIFIER = new OptionalParameter<>();

  public static final ConfigurationModule CONF = new EvaluatorConfiguration()
      .bindSetEntry(Clock.RuntimeStartHandler.class, EvaluatorRuntime.RuntimeStartHandler.class)
      .bindSetEntry(Clock.RuntimeStopHandler.class, EvaluatorRuntime.RuntimeStopHandler.class)
      .bindConstructor(ExecutorService.class, ExecutorServiceConstructor.class)
      .bindNamedParameter(DriverRemoteIdentifier.class, DRIVER_REMOTE_IDENTIFIER)
      .bindNamedParameter(EvaluatorIdentifier.class, EVALUATOR_IDENTIFIER)
      .bindNamedParameter(HeartbeatPeriod.class, HEARTBEAT_PERIOD)
      .bindNamedParameter(RootContextConfiguration.class, ROOT_CONTEXT_CONFIGURATION)
      .bindNamedParameter(InitialTaskConfiguration.class, TASK_CONFIGURATION)
      .bindNamedParameter(RootServiceConfiguration.class, ROOT_SERVICE_CONFIGURATION)
      .bindNamedParameter(ApplicationIdentifier.class, APPLICATION_IDENTIFIER)
      .build();
}
