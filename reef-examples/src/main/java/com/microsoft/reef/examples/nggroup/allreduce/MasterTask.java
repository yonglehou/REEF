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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ModelDimensions;
import com.microsoft.reef.examples.nggroup.broadcast.parameters.ModelAllReducer;
import com.microsoft.reef.io.network.group.operators.AllReduce;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.GroupCommClient;
import com.microsoft.reef.io.network.nggroup.impl.AllReducer;
import com.microsoft.reef.task.Task;
import com.microsoft.tang.annotations.Parameter;

/**
 *
 */
public class MasterTask implements Task {

  private static final Logger LOG = Logger
    .getLogger(MasterTask.class.getName());

  private final CommunicationGroupClient communicationGroupClient;
  private final AllReducer<Vector> modelAllReducer;
  private final int dimensions;

  @Inject
  public MasterTask(final GroupCommClient groupCommClient,
    @Parameter(ModelDimensions.class) final int dimensions) {
    this.communicationGroupClient = groupCommClient
      .getCommunicationGroup(AllCommunicationGroup.class);
    this.modelAllReducer = (AllReducer<Vector>) communicationGroupClient
      .getAllReducer(ModelAllReducer.class);
    this.dimensions = dimensions;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    Vector model = new DenseVector(new double[] { 1, 0 });
    Vector newModel = null;
    final int numIters = 10;
    final long time1 = System.currentTimeMillis();
    for (int i = 0; i < numIters; i++) {
      System.out.println("Iter: " + i + " starts.");
      model.set(1, i);
      newModel = modelAllReducer.apply(model);
      if (modelAllReducer.isLastIterationFailed()) {
        System.out.println("Iter: " + i + " apply data fails.");
        i = i - modelAllReducer.getNumFailedIterations();
        newModel = null;
      } else {
        System.out.println("Iter: " + i + " apply data succeeds.");
        i = (int) newModel.get(1);
      }
      if (newModel != null) {
        StringBuffer sb = new StringBuffer();
        for (int j = 0; j < newModel.size(); j++) {
          sb.append(newModel.get(j) + ",");
        }
        System.out.println(sb);
      } else {
        System.out.println("The result is null.");
      }
    }
    final long time2 = System.currentTimeMillis();
    System.out.println("Allreduce vector of dimensions " + dimensions
      + " took " + (time2 - time1) / (numIters * 1000.0) + " secs");
    return null;
  }
}
