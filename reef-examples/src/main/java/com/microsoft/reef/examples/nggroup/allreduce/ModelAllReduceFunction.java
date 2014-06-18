/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.examples.nggroup.allreduce;

import javax.inject.Inject;

import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;

/**
 *
 */
public class ModelAllReduceFunction implements ReduceFunction<Vector> {

  @Inject
  public ModelAllReduceFunction() {
  }

  @Override
  public Vector apply(final Iterable<Vector> elements) {
    int count = 0;
    final Vector model = new DenseVector(new double[] { 0 });
    for (Vector element : elements) {
      model.add(element);
      count++;
    }
    System.out.println("AllReduce Size: " + count);
    return model;
  }
}
