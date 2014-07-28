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
package com.microsoft.reef.task;

import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.annotations.audience.TaskSide;

import java.util.concurrent.Callable;

/**
 * The interface for Tasks.
 * <p/>
 * This interface is to be implemented for Tasks.
 * <p/>
 * The main entry point for a Task is the call() method inherited from
 * {@link Callable}. The REEF Evaluator will call this method in order to run
 * the Task. The byte[] returned by it will be pushed to the Job Driver.
 */
@TaskSide
@Public
public interface Task {

  /**
   * Called by the resourcemanager harness to execute the task.
   *
   * @param memento the memento objected passed down by the driver.
   * @return the user defined return value
   * @throws Exception whenever the Task encounters an unsolved issue.
   *                   This Exception will be thrown at the Driver's event handler.
   */
  public byte[] call(final byte[] memento) throws Exception;
}
