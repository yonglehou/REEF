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
import com.microsoft.reef.examples.nggroup.bgd.parameters.DescentDirectionBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Dimensions;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Iterations;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Lambda;
import com.microsoft.reef.examples.nggroup.bgd.parameters.LineSearchEvaluationsReducer;
import com.microsoft.reef.examples.nggroup.bgd.parameters.LossAndGradientReducer;
import com.microsoft.reef.examples.nggroup.bgd.parameters.MinEtaBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ModelAndDescentDirectionBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ModelBroadcaster;
import com.microsoft.reef.io.Tuple;
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
  private final Reduce.Receiver<Pair<Pair<Double,Integer>, Vector>> lossAndGradientReducer;
  private final Broadcast.Sender<Pair<Vector,Vector>> modelAndDescentDirectionBroadcaster;
  private final Broadcast.Sender<Vector> descentDriectionBroadcaster;
  private final Reduce.Receiver<Pair<Vector,Integer>> lineSearchEvaluationsReducer;
  private final Broadcast.Sender<Double> minEtaBroadcaster;
  private final int dimensions;
  private final boolean ignoreAndContinue = false;
  private final com.microsoft.reef.examples.nggroup.bgd.StepSizes ts;
  private final double lambda;
  private final int maxIters;

  @Inject
  public MasterTask(
      final GroupCommClient groupCommClient,
      @Parameter(Dimensions.class) final int dimensions,
      @Parameter(Lambda.class) final double lambda,
      @Parameter(Iterations.class) final int maxIters,
      final StepSizes ts){
    this.lambda = lambda;
    this.maxIters = maxIters;
    this.ts = ts;
    this.communicationGroupClient = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.controlMessageBroadcaster = communicationGroupClient.getBroadcastSender(ControlMessageBroadcaster.class);
    this.modelBroadcaster = communicationGroupClient.getBroadcastSender(ModelBroadcaster.class);
    this.lossAndGradientReducer = communicationGroupClient.getReduceReceiver(LossAndGradientReducer.class);
    this.modelAndDescentDirectionBroadcaster = communicationGroupClient.getBroadcastSender(ModelAndDescentDirectionBroadcaster.class);
    this.descentDriectionBroadcaster = communicationGroupClient.getBroadcastSender(DescentDirectionBroadcaster.class);
    this.lineSearchEvaluationsReducer = communicationGroupClient.getReduceReceiver(LineSearchEvaluationsReducer.class);
    this.minEtaBroadcaster = communicationGroupClient.getBroadcastSender(MinEtaBroadcaster.class);
    this.dimensions = dimensions;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    final ArrayList<Double> losses = new ArrayList<>();
    final Codec<ArrayList<Double>> lossCodec = new SerializableCodec<ArrayList<Double>>();
    final Vector model = new DenseVector(dimensions);
    boolean sendModel = true;
    double minEta = 0;

    int iters = 1;
    while(true){
      Pair<Pair<Double,Integer>,Vector> lossAndGradient;
      do{
        if(sendModel) {
          System.out.println("ComputeGradientWithModel");
          controlMessageBroadcaster.send(ControlMessages.ComputeGradientWithModel);
          modelBroadcaster.send(model);
        }
        else {
          System.out.println("ComputeGradientWithMinEta");
          controlMessageBroadcaster.send(ControlMessages.ComputeGradientWithMinEta);
          minEtaBroadcaster.send(minEta);
        }
        lossAndGradient = lossAndGradientReducer.reduce();

        if(!chkAndUpdate()) {
          sendModel = false;
          break;
        }
        else {
          sendModel = true;
        }
      }while(true);

      final double loss = regularizeLoss(lossAndGradient.first.first, lossAndGradient.first.second, model);
      System.out.println("Loss: " + loss);
      final Vector gradient = regularizeGrad(lossAndGradient.second, lossAndGradient.first.second, model);
      System.out.println("Gradient: " + gradient);
      losses.add(loss);
      if(converged(iters, gradient.norm2())){
        System.out.println("Stop");
        controlMessageBroadcaster.send(ControlMessages.Stop);
        break;
      }

      final Vector descentDirection = getDescentDirection(gradient);
      System.out.println("DescentDirection: " + descentDirection);

      Pair<Vector,Integer> lineSearchEvals;
      do {
        if(sendModel) {
          System.out.println("DoLineSearchWithModel");
          controlMessageBroadcaster.send(ControlMessages.DoLineSearchWithModel);
          modelAndDescentDirectionBroadcaster.send(new Pair<>(model,
              descentDirection));
        }
        else {
          System.out.println("DoLineSearch");
          controlMessageBroadcaster.send(ControlMessages.DoLineSearch);
          descentDriectionBroadcaster.send(descentDirection);
        }
        lineSearchEvals = lineSearchEvaluationsReducer.reduce();
        System.out.println("LineSearchEvals: " + lineSearchEvals.first);
        if (!chkAndUpdate()) {
          sendModel = false;
          break;
        }
        else {
          sendModel = true;
        }
      } while (true);

      minEta = findMinEta(model,descentDirection,lineSearchEvals);
      descentDirection.scale(minEta);
      model.add(descentDirection);

      System.out.println("New Model: " + model);
      ++iters;
    }
    for (final Double loss : losses) {
      System.out.println(loss);
    }
    return lossCodec.encode(losses);
  }

  /**
   * @return
   */
  private boolean chkAndUpdate() {
    final GroupChanges changes = communicationGroupClient.getTopologyChanges();
    if(changes.exist() && !ignoreAndContinue){
      Log.info("There exist topology changes. Asking to update Topology");
      communicationGroupClient.updateTopology();
      return true;
    }
    else {
      Log.info("No changes in topology exist. So not updating topology");
      return false;
    }
  }

  /**
   * @param second
   * @param model
   * @param second2
   * @return
   */
  private Vector regularizeGrad(final Vector gradient, final int numEx, final Vector model) {
    gradient.scale(1.0/numEx);
    gradient.multAdd(lambda, model);
    return gradient;
  }

  /**
   * @param first
   * @param model
   * @return
   */
  private double regularizeLoss(final double loss, final int numEx, final Vector model) {
    return loss/numEx + ((lambda/2) * Math.pow(model.norm2(), 2.0));
  }

  /**
   * @param loss
   * @return
   */
  private boolean converged(final int iters, final double gradNorm) {
    return iters>=maxIters || Math.abs(gradNorm) <= 1e-3 ;
  }

  /**
   * @param lineSearchEvals
   * @return
   */
  private double findMinEta(final Vector model, final Vector descentDir, final Pair<Vector,Integer> lineSearchEvals) {
    final double[] t = ts.getT();
    int i = 0;
    for (final double d : t) {
      final Vector newModel = DenseVector.copy(model);
      newModel.multAdd(d, descentDir);
      final double loss = regularizeLoss(lineSearchEvals.first.get(i), lineSearchEvals.second, newModel);
      lineSearchEvals.first.set(i, loss);
      ++i;
    }
    System.out.println("Regularized LineSearchEvals: " + lineSearchEvals.first);
    final Tuple<Integer, Double> minTup = lineSearchEvals.first.min();
    System.out.println("MinTup: " + minTup);
    final double minT = t[minTup.getKey()];
    System.out.println("MinT: " + minT);
    return minT;
  }

  /**
   * @param gradient
   * @return
   */
  private Vector getDescentDirection(final Vector gradient) {
    gradient.scale(-1);
    return gradient;
  }

}
