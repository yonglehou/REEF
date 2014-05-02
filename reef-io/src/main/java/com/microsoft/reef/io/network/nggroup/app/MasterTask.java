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
package com.microsoft.reef.io.network.nggroup.app;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroup;
import com.microsoft.reef.io.network.nggroup.api.GroupChanges;
import com.microsoft.reef.io.network.nggroup.api.GroupCommClient;
import com.microsoft.reef.io.network.nggroup.app.parameters.AllCommunicationGroup;
import com.microsoft.reef.io.network.nggroup.app.parameters.ControlMessageBroadcaster;
import com.microsoft.reef.io.network.nggroup.app.parameters.LineSearchEvaluationsReducer;
import com.microsoft.reef.io.network.nggroup.app.parameters.LossAndGradientReducer;
import com.microsoft.reef.io.network.nggroup.app.parameters.ModelAndDescentDirectionBroadcaster;
import com.microsoft.reef.io.network.nggroup.app.parameters.ModelBroadcaster;
import com.microsoft.reef.io.network.util.Utils.Pair;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.reef.task.Task;

/**
 * 
 */
public class MasterTask implements Task {
  private final CommunicationGroup communicationGroup;
  private final Broadcast.Sender<ControlMessages> controlMessageBroadcaster;
  private final Broadcast.Sender<Vector> modelBroadcaster;
  private final Reduce.Receiver<Pair<Double, Vector>> lossAndGradientReducer;
  private final Broadcast.Sender<Pair<Vector,Vector>> modelAndDescentDirectionBroadcaster;
  private final Reduce.Receiver<Vector> lineSearchEvaluationsReducer;
  private int numberOfReceivers;
  private final boolean ignoreAndContinue = true;
  
  @Inject
  public MasterTask(GroupCommClient groupCommClient, int numberOfReceivers){
    communicationGroup = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    controlMessageBroadcaster = communicationGroup.getBroadcastSender(ControlMessageBroadcaster.class);
    modelBroadcaster = communicationGroup.getBroadcastSender(ModelBroadcaster.class);
    lossAndGradientReducer = communicationGroup.getReduceReceiver(LossAndGradientReducer.class);
    modelAndDescentDirectionBroadcaster = communicationGroup.getBroadcastSender(ModelAndDescentDirectionBroadcaster.class);
    lineSearchEvaluationsReducer = communicationGroup.getReduceReceiver(LineSearchEvaluationsReducer.class);
    this.numberOfReceivers = numberOfReceivers;
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    
    communicationGroup.waitFor(numberOfReceivers,30,TimeUnit.SECONDS);
    List<Double> losses = new ArrayList<>();
    Codec<List<Double>> lossCodec = null;
    Vector model = null;
    while(true){
      controlMessageBroadcaster.send(ControlMessages.ComputeGradient);
      modelBroadcaster.send(model);
      Pair<Double,Vector> lossAndGradient = lossAndGradientReducer.reduce();
      GroupChanges changes = communicationGroup.synchronize();
      if(changes.exist() && !ignoreAndContinue){
        communicationGroup.waitFor(numberOfReceivers,30,TimeUnit.SECONDS);
        continue;
      }
      
      losses.add(lossAndGradient.first);
      Vector descentDirection = getDescentDirection(lossAndGradient.second);
      controlMessageBroadcaster.send(ControlMessages.DoLineSearch);
      modelAndDescentDirectionBroadcaster.send(new Pair<>(model, descentDirection));
      Vector lineSearchEvals = lineSearchEvaluationsReducer.reduce();
      changes = communicationGroup.synchronize();
      if(changes.exist() && !ignoreAndContinue){
        communicationGroup.waitFor(numberOfReceivers,30,TimeUnit.SECONDS);
        continue;
      }
      double minEta = findMinEta(lineSearchEvals);
      descentDirection.scale(minEta);
      model.add(descentDirection);
      if(converged(model)){
        controlMessageBroadcaster.send(ControlMessages.Stop);
        break;
      }
    }
    return lossCodec.encode(losses);
  }

  /**
   * @param model
   * @return
   */
  private boolean converged(Vector model) {
    // TODO Auto-generated method stub
    return false;
  }

  /**
   * @param lineSearchEvals
   * @return
   */
  private double findMinEta(Vector lineSearchEvals) {
    // TODO Auto-generated method stub
    return 0;
  }

  /**
   * @param second
   * @return
   */
  private Vector getDescentDirection(Vector second) {
    // TODO Auto-generated method stub
    return null;
  }

}
