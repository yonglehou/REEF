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

import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.util.Utils.Pair;

/**
 *
 */
public class ModelReduceFunction implements
  ReduceFunction<Pair<Integer, Vector>> {

  @Inject
  public ModelReduceFunction() {
  }

  @Override
  public Pair<Integer, Vector> apply(final Iterable<Pair<Integer, Vector>> evals) {
    Vector model = null;
    int max = Integer.MIN_VALUE;
    for (final Pair<Integer, Vector> eval : evals) {
      if (eval.first.intValue() > max) {
        max = eval.first.intValue();
        model = eval.second;
      }
    }
    return new Pair<>(max, model);
  }
}
