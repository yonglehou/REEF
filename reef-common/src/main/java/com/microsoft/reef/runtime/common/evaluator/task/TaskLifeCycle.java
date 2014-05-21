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
package com.microsoft.reef.runtime.common.evaluator.task;

import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.task.events.TaskStart;
import com.microsoft.reef.task.events.TaskStop;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.Set;

/**
 * Convenience class to send task start and stop events.
 */
final class TaskLifeCycle {

  private final Set<EventHandler<TaskStop>> taskStopHandlers;
  private final Set<EventHandler<TaskStart>> taskStartHandlers;
  private final TaskStart taskStart;
  private final TaskStop taskStop;

  @Inject
  TaskLifeCycle(final @Parameter(TaskConfigurationOptions.StopHandlers.class) Set<EventHandler<TaskStop>> taskStopHandlers,
                final @Parameter(TaskConfigurationOptions.StartHandlers.class) Set<EventHandler<TaskStart>> taskStartHandlers,
                final TaskStartImpl taskStart,
                final TaskStopImpl taskStop) {
    this.taskStopHandlers = taskStopHandlers;
    this.taskStartHandlers = taskStartHandlers;
    this.taskStart = taskStart;
    this.taskStop = taskStop;
  }

  public void start() {
    for (final EventHandler<TaskStart> startHandler : this.taskStartHandlers) {
      startHandler.onNext(this.taskStart);
    }
  }

  public void stop() {
    for (final EventHandler<TaskStop> stopHandler : this.taskStopHandlers) {
      stopHandler.onNext(this.taskStop);
    }
  }


}
