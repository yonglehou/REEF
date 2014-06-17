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
package com.microsoft.reef.examples.nggroup.broadcast;

import java.util.Random;

import javax.inject.Inject;

import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ControlMessageBroadcaster;
import com.microsoft.reef.examples.nggroup.broadcast.parameters.FailureProbability;
import com.microsoft.reef.examples.nggroup.broadcast.parameters.ModelBroadcaster;
import com.microsoft.reef.examples.nggroup.broadcast.parameters.ModelReceiveAckReducer;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.GroupCommClient;
import com.microsoft.reef.task.Task;
import com.microsoft.tang.annotations.Parameter;

/**
 *
 */
public class SlaveTask implements Task {
  private final CommunicationGroupClient communicationGroupClient;
  private final Broadcast.Receiver<ControlMessages> controlMessageBroadcaster;
  private final Broadcast.Receiver<Vector> modelBroadcaster;
  private final Reduce.Sender<Boolean> modelReceiveAckReducer;
  private final double failProb;
  private final Random r;
  private int iter = 0;

  @Inject
  public SlaveTask(
      final GroupCommClient groupCommClient,
      @Parameter(FailureProbability.class) final double failProb){
    this.failProb = failProb;
    this.communicationGroupClient = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.controlMessageBroadcaster = communicationGroupClient.getBroadcastReceiver(ControlMessageBroadcaster.class);
    this.modelBroadcaster = communicationGroupClient.getBroadcastReceiver(ModelBroadcaster.class);
    this.modelReceiveAckReducer = communicationGroupClient.getReduceSender(ModelReceiveAckReducer.class);
    r = new Random();
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    boolean stop = false;
    while(!stop){
      final ControlMessages controlMessage = controlMessageBroadcaster.receive();
      switch(controlMessage){
      case Stop:
        stop = true;
        break;

      case ReceiveModel:
        modelBroadcaster.receive();
        if(iter==3 && r.nextDouble()<=failProb) {
          throw new RuntimeException("Simulated Failure");
        }
        modelReceiveAckReducer.send(true);
        ++iter;
        break;

        default:
          break;
      }
    }
    return null;
  }
}
