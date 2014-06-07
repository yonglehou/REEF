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

import javax.inject.Inject;

import org.mortbay.log.Log;

import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ControlMessageBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Dimensions;
import com.microsoft.reef.examples.nggroup.bgd.parameters.NumberOfReceivers;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.GroupChanges;
import com.microsoft.reef.io.network.nggroup.api.GroupCommClient;
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
//  private final Broadcast.Sender<Vector> modelBroadcaster;
//  private final Reduce.Receiver<Pair<Double, Vector>> lossAndGradientReducer;
//  private final Broadcast.Sender<Pair<Vector,Vector>> modelAndDescentDirectionBroadcaster;
//  private final Reduce.Receiver<Vector> lineSearchEvaluationsReducer;
  private final int numberOfReceivers;
  private final int dimensions;
  private final boolean ignoreAndContinue = false;
  private final GroupCommClient groupCommClient;

  @Inject
  public MasterTask(
      final GroupCommClient groupCommClient,
      @Parameter(NumberOfReceivers.class) final int numberOfReceivers,
      @Parameter(Dimensions.class) final int dimensions){
    this.groupCommClient = groupCommClient;
    this.communicationGroupClient = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.controlMessageBroadcaster = communicationGroupClient.getBroadcastSender(ControlMessageBroadcaster.class);
//    this.modelBroadcaster = communicationGroupClient.getBroadcastSender(ModelBroadcaster.class);
//    this.lossAndGradientReducer = communicationGroupClient.getReduceReceiver(LossAndGradientReducer.class);
//    this.modelAndDescentDirectionBroadcaster = communicationGroupClient.getBroadcastSender(ModelAndDescentDirectionBroadcaster.class);
//    this.lineSearchEvaluationsReducer = communicationGroupClient.getReduceReceiver(LineSearchEvaluationsReducer.class);
    this.numberOfReceivers = numberOfReceivers;
    this.dimensions = dimensions;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    try{
      final ArrayList<Double> losses = new ArrayList<>();
      final Codec<ArrayList<Double>> lossCodec = new SerializableCodec<ArrayList<Double>>();
      final Vector model = new DenseVector(dimensions);
      controlMessageBroadcaster.send(ControlMessages.ComputeGradient);
      final GroupChanges changes = communicationGroupClient.getTopologyChanges();
      if(changes.exist()) {
        Log.info("There exist topology changes. Asking to update Topology");
        communicationGroupClient.updateTopology();
      } else {
        Log.info("No changes in topology exist. So not updating topology");
      }
      controlMessageBroadcaster.send(ControlMessages.Stop);
      return lossCodec.encode(losses);
//      modelBroadcaster.send(model);
//      final Pair<Double,Vector> lossAndGradient = lossAndGradientReducer.reduce();
//      GroupChanges changes = communicationGroupClient.getTopologyChanges();
//      communicationGroupClient.updateTopology();


      /*while(true){
        controlMessageBroadcaster.send(ControlMessages.ComputeGradient);
        modelBroadcaster.send(model);
        final Pair<Double,Vector> lossAndGradient = lossAndGradientReducer.reduce();
        GroupChanges changes = communicationGroupClient.synchronize();
        if(changes.exist() && !ignoreAndContinue){
          communicationGroupClient.waitFor(numberOfReceivers,30,TimeUnit.SECONDS);
          continue;
        }

        losses.add(lossAndGradient.first);
        final Vector descentDirection = getDescentDirection(lossAndGradient.second);
        controlMessageBroadcaster.send(ControlMessages.DoLineSearch);
        modelAndDescentDirectionBroadcaster.send(new Pair<>(model, descentDirection));
        final Vector lineSearchEvals = lineSearchEvaluationsReducer.reduce();
        changes = communicationGroupClient.synchronize();
        if(changes.exist() && !ignoreAndContinue){
          communicationGroupClient.waitFor(numberOfReceivers,30,TimeUnit.SECONDS);
          continue;
        }
        final double minEta = findMinEta(lineSearchEvals);
        descentDirection.scale(minEta);
        model.add(descentDirection);
        if(converged(model)){
          controlMessageBroadcaster.send(ControlMessages.Stop);
          break;
        }
      }*/
//      return lossCodec.encode(losses);
    }catch(final /*TimeoutException*/ Exception e){
//      controlMessageBroadcaster.send(ControlMessages.Stop);
      throw e;
    }
  }

  /**
   * @param model
   * @return
   */
  private boolean converged(final Vector model) {
    // TODO Auto-generated method stub
    return false;
  }

  /**
   * @param lineSearchEvals
   * @return
   */
  private double findMinEta(final Vector lineSearchEvals) {
    // TODO Auto-generated method stub
    return 0;
  }

  /**
   * @param second
   * @return
   */
  private Vector getDescentDirection(final Vector second) {
    // TODO Auto-generated method stub
    return null;
  }

}
