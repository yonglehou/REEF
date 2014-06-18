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
import com.microsoft.reef.examples.nggroup.bgd.parameters.Dimensions;
import com.microsoft.reef.examples.nggroup.broadcast.parameters.ModelAllReducer;
import com.microsoft.reef.io.network.group.operators.AllReduce;
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
  private final AllReduce<Vector> modelAllReducer;
  private final int dimensions;

  @Inject
  public SlaveTask(final GroupCommClient groupCommClient,
    @Parameter(Dimensions.class) final int dimensions) {
    this.communicationGroupClient =
      groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.modelAllReducer =
      communicationGroupClient.getAllReducer(ModelAllReducer.class);
    this.dimensions = dimensions;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    Vector model = new DenseVector(new double[] { 1 });
    final long time1 = System.currentTimeMillis();
    final int numIters = 3;
    for (int i = 0; i < numIters; i++) {
      System.out.println("Iter: " + i + " starts.");
      model = modelAllReducer.apply(model);
      System.out.println("Iter: " + i + " apply data");
    }
    final long time2 = System.currentTimeMillis();
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < model.size(); i++) {
      sb.append(model.get(i) + ",");
    }
    System.out.println(sb);
    LOG.info("Allreduce vector of dimensions " + dimensions + " took "
      + (time2 - time1) / (numIters * 1000.0) + " secs");
    return null;
  }
}
