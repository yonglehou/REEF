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
package com.microsoft.reef.examples.nggroup.bgdallreduce;

import javax.inject.Inject;

import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;

public class ControlMessageReduceFunction implements
  ReduceFunction<ControlMessage> {

  @Inject
  public ControlMessageReduceFunction() {
  }

  @Override
  public ControlMessage apply(final Iterable<ControlMessage> evals) {
    int maxIte = Integer.MIN_VALUE;
    int maxOp = Integer.MIN_VALUE;
    int minIte = Integer.MAX_VALUE;
    int minOp = Integer.MAX_VALUE;
    boolean stop = false;
    // Task ID matches with
    // the task which contains
    // the max iteration and the max operation
    String taskID = null;
    boolean syncData = false;
    for (final ControlMessage eval : evals) {
      if (maxIte < eval.iteration) {
        maxIte = eval.iteration;
        maxOp = eval.operation;
        taskID = eval.taskID;
      }
      if (maxIte == eval.iteration) {
        if (maxOp < eval.operation) {
          maxOp = eval.operation;
          taskID = eval.taskID;
        }
      }
      if (maxIte == eval.iteration && maxOp < eval.operation) {
        if (taskID.compareTo(eval.taskID) < 0) {
          taskID = eval.taskID;
        }
      }
      if (minIte > eval.iteration) {
        minIte = eval.iteration;
        minOp = eval.operation;
      }
      if (minIte == eval.iteration) {
        if (minOp > eval.operation) {
          minOp = eval.operation;
        }
      }
      // Update to true if any eval says its syncData is true.
      if (!syncData && eval.syncData) {
        syncData = true;
      }
      if (!stop && eval.stop) {
        stop = true;
      }
    }
    // If not in the same iteration, definitely set syncData to true.
    if (maxIte != minIte) {
      syncData = true;
    }
    // If in the same iteration, but not the same op, set syncData to true.
    if (maxIte == minIte && maxOp != minOp) {
      syncData = true;
    }
    // If iteration and op are all equal,see which eval says true.
    return new ControlMessage(maxIte, maxOp, taskID, syncData, stop);
  }
}
