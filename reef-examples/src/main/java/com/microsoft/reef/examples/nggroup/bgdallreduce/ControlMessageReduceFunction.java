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
import com.microsoft.reef.io.network.util.Utils.Pair;

public class ControlMessageReduceFunction implements
  ReduceFunction<Pair<Pair<Integer, Integer>, Pair<String, Boolean>>> {

  @Inject
  public ControlMessageReduceFunction() {
  }

  @Override
  public Pair<Pair<Integer, Integer>, Pair<String, Boolean>> apply(
    final Iterable<Pair<Pair<Integer, Integer>, Pair<String, Boolean>>> evals) {
    // Find the max number of integer,
    // this is where the operation should resume.
    // If the max is changed, set sendModel to be true.
    int maxIte = Integer.MIN_VALUE;
    int maxOp = Integer.MIN_VALUE;
    int minIte = Integer.MAX_VALUE;
    int minOp = Integer.MAX_VALUE;
    String taskID = null;
    boolean sendModel = false;
    for (final Pair<Pair<Integer, Integer>, Pair<String, Boolean>> eval : evals) {
      if (maxIte < eval.first.first.intValue()) {
        maxIte = eval.first.first.intValue();
        maxOp = eval.first.second.intValue();
        // Task ID matches with
        // the task which contains
        // the max iterations and the max op
        taskID = eval.second.first;
      }
      if (maxIte == eval.first.first.intValue()) {
        if (maxOp < eval.first.second.intValue()) {
          maxOp = eval.first.second.intValue();
          taskID = eval.second.first;
        }
      }
      if (minIte > eval.first.first.intValue()) {
        minIte = eval.first.first.intValue();
        minOp = eval.first.second.intValue();
      }
      if (minIte == eval.first.first.intValue()) {
        if (minOp > eval.first.second.intValue()) {
          minOp = eval.first.second.intValue();
        }
      }
      // Update to true if any eval says its sendModel is true.
      if (!sendModel && eval.second.second) {
        sendModel = true;
      }
    }
    // If not in the same iteration, definitely set sendModel to true.
    if (maxIte != minIte) {
      sendModel = true;
    }
    // If in the same iteration, but not the same op, set sendModel to true.
    if (maxIte == minIte && maxOp != minOp) {
      sendModel = true;
    }
    // If iteration and op are all equal,see which eval says true.
    return new Pair<Pair<Integer, Integer>, Pair<String, Boolean>>(new Pair<>(
      maxIte, maxOp), new Pair<>(taskID, sendModel));
  }
}
