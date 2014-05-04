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
package com.microsoft.reef.driver.context;

import com.microsoft.reef.evaluator.context.parameters.ContextStartHandlers;
import com.microsoft.reef.evaluator.context.parameters.ContextStopHandlers;
import com.microsoft.reef.evaluator.context.parameters.Services;
import com.microsoft.reef.task.events.TaskStart;
import com.microsoft.reef.task.events.TaskStop;
import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.evaluator.context.events.ContextStart;
import com.microsoft.reef.evaluator.context.events.ContextStop;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.OptionalImpl;
import com.microsoft.tang.formats.OptionalParameter;
import com.microsoft.wake.EventHandler;

/**
 * Configuration module for services. The configuration created here can be passed alongside a ContextConfiguration
 * to form a context. Different from bindings made in the ContextConfiguration, those made here will be passed along
 * to child context.
 */
public class ServiceConfiguration extends ConfigurationModuleBuilder {

  /**
   * A set of services to instantiate. All classes given here will be instantiated in the context, and their references
   * will be made available to child context and tasks.
   */
  public static final OptionalParameter<Object> SERVICES = new OptionalParameter<>();

  /**
   * Event handler for context start. Defaults to logging if not bound.
   */
  public static final OptionalImpl<EventHandler<ContextStart>> ON_CONTEXT_STARTED = new OptionalImpl<>();

  /**
   * Event handler for context stop. Defaults to logging if not bound.
   */
  public static final OptionalImpl<EventHandler<ContextStop>> ON_CONTEXT_STOP = new OptionalImpl<>();

  /**
   * Event handlers to be informed right before a Task enters its call() method.
   */
  public static final OptionalImpl<EventHandler<TaskStart>> ON_TASK_STARTED = new OptionalImpl<>();

  /**
   * Event handlers to be informed right after a Task exits its call() method.
   */
  public static final OptionalImpl<EventHandler<TaskStop>> ON_TASK_STOP = new OptionalImpl<>();

  /**
   * ConfigurationModule for services.
   */
  public static final ConfigurationModule CONF = new ServiceConfiguration()
      .bindSetEntry(Services.class, SERVICES)
      .bindSetEntry(ContextStartHandlers.class, ON_CONTEXT_STARTED)
      .bindSetEntry(ContextStopHandlers.class, ON_CONTEXT_STOP)
      .bindSetEntry(TaskConfigurationOptions.StartHandlers.class, ON_TASK_STARTED)
      .bindSetEntry(TaskConfigurationOptions.StopHandlers.class, ON_TASK_STOP)
      .build();

}
