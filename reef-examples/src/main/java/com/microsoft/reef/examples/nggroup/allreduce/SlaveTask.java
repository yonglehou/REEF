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

import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ModelDimensions;
import com.microsoft.reef.io.network.group.operators.AllReduce;
import com.microsoft.reef.io.network.group.operators.AllReduceResult;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.GroupCommClient;
import com.microsoft.reef.task.Task;
import com.microsoft.tang.annotations.Parameter;

/**
 *
 */
public class SlaveTask implements Task {
  private static final Logger LOG = Logger.getLogger(SlaveTask.class.getName());

  private final CommunicationGroupClient communicationGroupClient;
  private final AllReduce<Integer> controlMsgAllReducer;
  private final AllReduce<Vector> modelAllReducer;
  private final int dimensions;

  @Inject
  public SlaveTask(final GroupCommClient groupCommClient,
    @Parameter(ModelDimensions.class) final int dimensions) {
    this.communicationGroupClient =
      groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.controlMsgAllReducer =
      communicationGroupClient.getAllReducer(ControlMessageAllReducer.class);
    this.modelAllReducer =
      communicationGroupClient.getAllReducer(ModelAllReducer.class);
    this.dimensions = dimensions;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    final long time1 = System.currentTimeMillis();
    Vector model = new DenseVector(new double[] { 1, 1, 1, 1 });
    final int numIters = 50;
    int ite = 0;
    int op = 0;
    while (ite < numIters) {
      checkAndUpdate();
      if (Math.random() < 0.1) {
        System.out.println("Simulated Failure");
        throw new RuntimeException("Simulated Failure");
      }
      System.out.println("SYNC ITERATION ");
      AllReduceResult<Integer> recvIte = controlMsgAllReducer.apply(ite);
      if (!recvIte.isEmpty()) {
        ite = recvIte.getValue().intValue();
        System.out.println("GET ITERATION " + ite);
        op = 1;
      } else {
        op = 0;
      }
      if (op == 1) {
        System.out.println("ITERATION " + ite + " STARTS.");
        AllReduceResult<Vector> newModel = modelAllReducer.apply(model);
        if (!newModel.isEmpty()) {
          StringBuffer sb = new StringBuffer();
          sb.append('[');
          for (int j = 0; j < newModel.getValue().size(); j++) {
            sb.append(newModel.getValue().get(j) + ",");
          }
          sb.setCharAt(sb.length() - 1, ']');
          System.out.println("RESULT " + sb);
          ite++;
        } else {
          System.out.println("RESULT IS NULL.");
        }
      }
    }
    final long time2 = System.currentTimeMillis();
    System.out.println("Allreduce vector of dimensions " + dimensions
      + " took " + (time2 - time1) / (numIters * 1000.0) + " secs");
    return null;
  }

  private void checkAndUpdate() {
    communicationGroupClient.checkIteration();
    communicationGroupClient.updateIteration();
  }
}
