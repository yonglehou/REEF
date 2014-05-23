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
package com.microsoft.reef.examples.nggroup.bgd;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ControlMessageBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Dimensions;
import com.microsoft.reef.examples.nggroup.bgd.parameters.LineSearchEvaluationsReducer;
import com.microsoft.reef.examples.nggroup.bgd.parameters.LossAndGradientReducer;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ModelAndDescentDirectionBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ModelBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.NumberOfReceivers;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.GroupChanges;
import com.microsoft.reef.io.network.nggroup.api.GroupCommClient;
import com.microsoft.reef.io.network.util.Utils.Pair;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.reef.io.serialization.SerializableCodec;
import com.microsoft.reef.task.Task;
import com.microsoft.tang.annotations.Parameter;

/**
 * 
 */
public class MasterTask implements Task {
  private final CommunicationGroupClient communicationGroupClient;
  private final Broadcast.Sender<ControlMessages> controlMessageBroadcaster;
  private final Broadcast.Sender<Vector> modelBroadcaster;
  private final Reduce.Receiver<Pair<Double, Vector>> lossAndGradientReducer;
  private final Broadcast.Sender<Pair<Vector,Vector>> modelAndDescentDirectionBroadcaster;
  private final Reduce.Receiver<Vector> lineSearchEvaluationsReducer;
  private int numberOfReceivers;
  private final int dimensions;
  private final boolean ignoreAndContinue = false;
  
  @Inject
  public MasterTask(
      GroupCommClient groupCommClient, 
      @Parameter(NumberOfReceivers.class) int numberOfReceivers,
      @Parameter(Dimensions.class) int dimensions){
    communicationGroupClient = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    controlMessageBroadcaster = communicationGroupClient.getBroadcastSender(ControlMessageBroadcaster.class);
    modelBroadcaster = communicationGroupClient.getBroadcastSender(ModelBroadcaster.class);
    lossAndGradientReducer = communicationGroupClient.getReduceReceiver(LossAndGradientReducer.class);
    modelAndDescentDirectionBroadcaster = communicationGroupClient.getBroadcastSender(ModelAndDescentDirectionBroadcaster.class);
    lineSearchEvaluationsReducer = communicationGroupClient.getReduceReceiver(LineSearchEvaluationsReducer.class);
    this.numberOfReceivers = numberOfReceivers;
    this.dimensions = dimensions;
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    return null;
    /*try{
      communicationGroupClient.waitFor(numberOfReceivers,30,TimeUnit.SECONDS);
    
      ArrayList<Double> losses = new ArrayList<>();
      Codec<ArrayList<Double>> lossCodec = new SerializableCodec<ArrayList<Double>>();
      Vector model = new DenseVector(dimensions);
      while(true){
        controlMessageBroadcaster.send(ControlMessages.ComputeGradient);
        modelBroadcaster.send(model);
        Pair<Double,Vector> lossAndGradient = lossAndGradientReducer.reduce();
        GroupChanges changes = communicationGroupClient.synchronize();
        if(changes.exist() && !ignoreAndContinue){
          communicationGroupClient.waitFor(numberOfReceivers,30,TimeUnit.SECONDS);
          continue;
        }
        
        losses.add(lossAndGradient.first);
        Vector descentDirection = getDescentDirection(lossAndGradient.second);
        controlMessageBroadcaster.send(ControlMessages.DoLineSearch);
        modelAndDescentDirectionBroadcaster.send(new Pair<>(model, descentDirection));
        Vector lineSearchEvals = lineSearchEvaluationsReducer.reduce();
        changes = communicationGroupClient.synchronize();
        if(changes.exist() && !ignoreAndContinue){
          communicationGroupClient.waitFor(numberOfReceivers,30,TimeUnit.SECONDS);
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
    }catch(TimeoutException e){
      controlMessageBroadcaster.send(ControlMessages.Stop);
      throw e;
    }*/
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
