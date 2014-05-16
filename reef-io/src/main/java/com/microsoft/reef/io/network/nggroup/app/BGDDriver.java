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

import javax.inject.Inject;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.io.data.loading.api.DataLoadingService;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroup;
import com.microsoft.reef.io.network.nggroup.api.GroupCommDriver;
import com.microsoft.reef.io.network.nggroup.app.parameters.AllCommunicationGroup;
import com.microsoft.reef.io.network.nggroup.app.parameters.ControlMessageBroadcaster;
import com.microsoft.reef.io.network.nggroup.app.parameters.LineSearchEvaluationsReducer;
import com.microsoft.reef.io.network.nggroup.app.parameters.LossAndGradientReducer;
import com.microsoft.reef.io.network.nggroup.app.parameters.ModelAndDescentDirectionBroadcaster;
import com.microsoft.reef.io.network.nggroup.app.parameters.ModelBroadcaster;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import com.microsoft.reef.io.network.util.Utils.Pair;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.Codec;

/**
 * 
 */
@DriverSide
@Unit
public class BGDDriver {
  
  private final DataLoadingService dataLoadingService;
  
  private final GroupCommDriver groupCommDriver;
  
  private final CommunicationGroup allCommGroup; 
  
  @Inject
  public BGDDriver(
      DataLoadingService dataLoadingService,
      GroupCommDriver groupCommDriver){
    this.dataLoadingService = dataLoadingService;
    this.groupCommDriver = groupCommDriver;
    this.allCommGroup = this.groupCommDriver.newCommunicationGroup(AllCommunicationGroup.class);
    Codec<ControlMessages> controlMsgCodec = null;
    Codec<Vector> modelCodec = null;
    Codec<Pair<Double,Vector>> lossAndGradientCodec = null;
    Codec<Pair<Vector,Vector>> modelAndDesDirCodec = null;
    Codec<Vector> lineSearchCodec = null;
    ReduceFunction<Pair<Double,Vector>> lossAndGradientReduceFunction = null;
    ReduceFunction<Vector> lineSearchReduceFunction = null;
    allCommGroup
      .addBroadcast(ControlMessageBroadcaster.class, 
          BroadcastOperatorSpec
            .newBuilder()
            .setSenderId("MasterTask")
            .setDataCodecClass(controlMsgCodec.getClass())
            .build())
      .addBroadcast(ModelBroadcaster.class,
          BroadcastOperatorSpec
            .newBuilder()
            .setSenderId("MasterTask")
            .setDataCodecClass(modelCodec.getClass())
            .build())
      .addReduce(LossAndGradientReducer.class, 
          ReduceOperatorSpec
            .newBuilder()
            .setDataCodecClass(lossAndGradientCodec.getClass())
            .setReduceFunctionClass(lossAndGradientReduceFunction.getClass())
            .build())
      .addBroadcast(ModelAndDescentDirectionBroadcaster.class, 
          BroadcastOperatorSpec
          .newBuilder()
          .setSenderId("MasterTask")
          .setDataCodecClass(modelAndDesDirCodec.getClass())
          .build())
      .addReduce(LineSearchEvaluationsReducer.class, 
          ReduceOperatorSpec
          .newBuilder()
          .setDataCodecClass(lineSearchCodec.getClass())
          .setReduceFunctionClass(lineSearchReduceFunction.getClass())
          .build())
      .finalize();
  }
  
  public class ContextActiveHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(ActiveContext activeContext) {
      /**
       * The active context can be either from
       * data loading service or after network
       * service has loaded contexts. So check
       * if the GroupCommDriver knows if it was
       * configured by one of the communication
       * groups
       */
      if(groupCommDriver.configured(activeContext)){
        if(!masterTaskSubmitted()){
          Configuration taskConf = allCommGroup.getConfiguration(
              TaskConfiguration.CONF
                .set(TaskConfiguration.IDENTIFIER, "MasterTask")
                .set(TaskConfiguration.TASK, MasterTask.class)
                .build());
          activeContext.submitTask(taskConf);
        }
        else{
          Configuration taskConf = allCommGroup.getConfiguration(
              TaskConfiguration.CONF
                .set(TaskConfiguration.IDENTIFIER, getSlaveId(activeContext))
                .set(TaskConfiguration.TASK, SlaveTask.class)
                .build());
          activeContext.submitTask(taskConf);
        }
      }
      else{
        activeContext.submitContextAndService(groupCommDriver.getContextConf(), groupCommDriver.getServiceConf());
      }
    }

    /**
     * @param activeContext
     * @return
     */
    private String getSlaveId(ActiveContext activeContext) {
      // TODO Auto-generated method stub
      return null;
    }

    /**
     * @return
     */
    private boolean masterTaskSubmitted() {
      // TODO Auto-generated method stub
      return false;
    }
  }
}
