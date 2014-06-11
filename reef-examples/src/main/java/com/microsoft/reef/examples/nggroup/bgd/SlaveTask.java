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
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.microsoft.reef.examples.nggroup.bgd.data.Example;
import com.microsoft.reef.examples.nggroup.bgd.data.parser.Parser;
import com.microsoft.reef.examples.nggroup.bgd.loss.LossFunction;
import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ControlMessageBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.LineSearchEvaluationsReducer;
import com.microsoft.reef.examples.nggroup.bgd.parameters.LossAndGradientReducer;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ModelAndDescentDirectionBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ModelBroadcaster;
import com.microsoft.reef.io.data.loading.api.DataSet;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.GroupCommClient;
import com.microsoft.reef.io.network.util.Utils.Pair;
import com.microsoft.reef.task.Task;

/**
 *
 */
public class SlaveTask implements Task {
  private final CommunicationGroupClient communicationGroup;
  private final Broadcast.Receiver<ControlMessages> controlMessageBroadcaster;
  private final Broadcast.Receiver<Vector> modelBroadcaster;
  private final Reduce.Sender<Pair<Pair<Double,Integer>, Vector>> lossAndGradientReducer;
  private final Broadcast.Receiver<Pair<Vector,Vector>> modelAndDescentDirectionBroadcaster;
  private final Reduce.Sender<Pair<Vector,Integer>> lineSearchEvaluationsReducer;
  private final GroupCommClient groupCommClient;
  private final List<Example> examples = new ArrayList<>();
  private final DataSet<LongWritable, Text> dataSet;
  private final Parser<String> parser;
  private final LossFunction lossFunction;
  private final StepSizes ts;

  @Inject
  public SlaveTask(
      final GroupCommClient groupCommClient,
      final DataSet<LongWritable, Text> dataSet,
      final Parser<String> parser,
      final LossFunction lossFunction,
      final StepSizes ts){
    this.groupCommClient = groupCommClient;
    this.dataSet = dataSet;
    this.parser = parser;
    this.lossFunction = lossFunction;
    this.ts = ts;
    communicationGroup = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    controlMessageBroadcaster = communicationGroup.getBroadcastReceiver(ControlMessageBroadcaster.class);
    modelBroadcaster = communicationGroup.getBroadcastReceiver(ModelBroadcaster.class);
    lossAndGradientReducer = communicationGroup.getReduceSender(LossAndGradientReducer.class);
    modelAndDescentDirectionBroadcaster = communicationGroup.getBroadcastReceiver(ModelAndDescentDirectionBroadcaster.class);
    lineSearchEvaluationsReducer = communicationGroup.getReduceSender(LineSearchEvaluationsReducer.class);
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

      case ComputeGradient:
        final Vector model = modelBroadcaster.receive();
        final Pair<Pair<Double,Integer>, Vector> lossAndGradient = computeLossAndGradient(model);
        lossAndGradientReducer.send(lossAndGradient);
        break;

      case DoLineSearch:
        final Pair<Vector,Vector> modelAndDescentDir = modelAndDescentDirectionBroadcaster.receive();
        final Pair<Vector,Integer> lineSearchEvals = lineSearchEvals(modelAndDescentDir);
        lineSearchEvaluationsReducer.send(lineSearchEvals);
        break;

        default:
          break;
      }
    }
    return null;
  }

  /**
   * @param modelAndDescentDir
   * @return
   */
  private Pair<Vector,Integer> lineSearchEvals(final Pair<Vector, Vector> modelAndDescentDir) {
    final Vector w = modelAndDescentDir.first;
    final Vector desDir = modelAndDescentDir.second;
    final Vector zed = new DenseVector(examples.size());
    final Vector ee = new DenseVector(examples.size());
    for (int i=0;i<examples.size();i++) {
      final Example example = examples.get(i);
      double f = example.predict(w);
      zed.set(i, f);
      f = example.predict(desDir);
      ee.set(i, f);
    }

    final double[] t = ts.getT();
    final Vector evaluations = new DenseVector(t.length);
    int i = 0;
    for (final double d : t) {
      double loss = 0;
      for (int j=0;j<examples.size();j++) {
        final Example example = examples.get(j);
        final double val = zed.get(j) + d * ee.get(j);
        loss += this.lossFunction.computeLoss(example.getLabel(), val);
      }
      evaluations.set(i++, loss);
    }
    return new Pair<>(evaluations,examples.size());
  }

  /**
   * @param model
   * @return
   */
  private Pair<Pair<Double,Integer>, Vector> computeLossAndGradient(final Vector model) {
    if(examples.isEmpty()) {
      loadData();
    }
    final Vector gradient = new DenseVector(model.size());
    double loss = 0.0;
    for (final Example example : examples) {
      final double f = example.predict(model);

      final double g = this.lossFunction.computeGradient(example.getLabel(), f);
      example.addGradient(gradient, g);
      loss += this.lossFunction.computeLoss(example.getLabel(), f);
    }
    return new Pair<>(new Pair<>(loss,examples.size()),gradient);
  }

  /**
   *
   */
  private void loadData() {
    for (final Pair<LongWritable, Text> examplePair : dataSet) {
      final Example example = parser.parse(examplePair.second.toString());
      examples.add(example);
    }
  }

}
