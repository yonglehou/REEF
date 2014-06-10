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

import javax.inject.Inject;

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
  private final Reduce.Sender<Pair<Double, Vector>> lossAndGradientReducer;
  private final Broadcast.Receiver<Pair<Vector,Vector>> modelAndDescentDirectionBroadcaster;
  private final Reduce.Sender<Vector> lineSearchEvaluationsReducer;
  private final GroupCommClient groupCommClient;

  @Inject
  public SlaveTask(final GroupCommClient groupCommClient, final DataSet<?, ?> dataSet){
    this.groupCommClient = groupCommClient;
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
        final Pair<Double, Vector> lossAndGradient = computeLossAndGradient(model);
        lossAndGradientReducer.send(lossAndGradient);
        break;

      case DoLineSearch:
        final Pair<Vector,Vector> modelAndDescentDir = modelAndDescentDirectionBroadcaster.receive();
        final Vector lineSearchEvals = lineSearchEvals(modelAndDescentDir);
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
  private Vector lineSearchEvals(final Pair<Vector, Vector> modelAndDescentDir) {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * @param model
   * @return
   */
  private Pair<Double, Vector> computeLossAndGradient(final Vector model) {
    return new Pair<Double, Vector>(0.0,model);
  }

}
