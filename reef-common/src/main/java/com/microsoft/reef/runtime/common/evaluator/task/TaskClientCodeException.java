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
package com.microsoft.reef.runtime.common.evaluator.task;

import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.InjectionException;

/**
 * Thrown by REEF's resourcemanager code when it catches an exception thrown by user code.
 */
public final class TaskClientCodeException extends Exception {

  private final String taskId;
  private final String contextId;

  /**
   * @param taskId    the id of the failed task.
   * @param contextId the ID of the context the failed Task was executing in.
   * @param message   the error message.
   * @param cause     the exception that caused the Task to fail.
   */
  public TaskClientCodeException(final String taskId,
                                 final String contextId,
                                 final String message,
                                 final Throwable cause) {
    super("Failure in task '" + taskId + "' in context '" + contextId + "': " + message, cause);
    this.taskId = taskId;
    this.contextId = contextId;
  }

  /**
   * @return the ID of the failed Task.
   */
  public String getTaskId() {
    return this.taskId;
  }

  /**
   * @return the ID of the context the failed Task was executing in.
   */
  public String getContextId() {
    return this.contextId;
  }

  /**
   * Extracts a task id from the given configuration.
   *
   * @param config
   * @return the task id in the given configuration.
   * @throws RuntimeException if the configuration can't be parsed.
   */
  public static String getTaskId(final Configuration config) {
    try {
      return Tang.Factory.getTang().newInjector(config).getNamedInstance(TaskConfigurationOptions.Identifier.class);
    } catch (final InjectionException ex) {
      throw new RuntimeException("Unable to determine task identifier. Giving up.", ex);
    }
  }
}
