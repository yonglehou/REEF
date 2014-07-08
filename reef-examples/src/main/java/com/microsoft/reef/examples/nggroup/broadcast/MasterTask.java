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
package com.microsoft.reef.examples.nggroup.broadcast;

import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.ControlMessageBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ModelDimensions;
import com.microsoft.reef.examples.nggroup.broadcast.parameters.ModelBroadcaster;
import com.microsoft.reef.examples.nggroup.broadcast.parameters.ModelReceiveAckReducer;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.GroupChanges;
import com.microsoft.reef.io.network.nggroup.api.GroupCommClient;
import com.microsoft.reef.task.Task;
import com.microsoft.tang.annotations.Parameter;
import org.mortbay.log.Log;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 *
 */
public class MasterTask implements Task {

  private static final Logger LOG = Logger.getLogger(MasterTask.class.getName());


  private final CommunicationGroupClient communicationGroupClient;
  private final Broadcast.Sender<ControlMessages> controlMessageBroadcaster;
  private final Broadcast.Sender<Vector> modelBroadcaster;
  private final Reduce.Receiver<Boolean> modelReceiveAckReducer;

  private final int dimensions;

  @Inject
  public MasterTask(
      final GroupCommClient groupCommClient,
      @Parameter(ModelDimensions.class) final int dimensions) {
    this.communicationGroupClient = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.controlMessageBroadcaster = communicationGroupClient.getBroadcastSender(ControlMessageBroadcaster.class);
    this.modelBroadcaster = communicationGroupClient.getBroadcastSender(ModelBroadcaster.class);
    this.modelReceiveAckReducer = communicationGroupClient.getReduceReceiver(ModelReceiveAckReducer.class);
    this.dimensions = dimensions;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    final Vector model = new DenseVector(dimensions);
    final long time1 = System.currentTimeMillis();
    final int numIters = 10;
    for (int i = 0; i < numIters; i++) {
      controlMessageBroadcaster.send(ControlMessages.ReceiveModel);
      modelBroadcaster.send(model);
      modelReceiveAckReducer.reduce();
      final GroupChanges changes = communicationGroupClient.getTopologyChanges();
      if (changes.exist()) {
        Log.info("There exist topology changes. Asking to update Topology");
        communicationGroupClient.updateTopology();
      } else {
        Log.info("No changes in topology exist. So not updating topology");
      }
    }
    final long time2 = System.currentTimeMillis();
    LOG.info("Broadcasting vector of dimensions " + dimensions + " took " + (time2 - time1) / (numIters * 1000.0) + " secs");
    controlMessageBroadcaster.send(ControlMessages.Stop);
    return null;
  }
}
