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

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.task.RunningTask;
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
import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.reef.io.network.util.Utils.Pair;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.ComparableIdentifier;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.IdentifierFactory;

/**
 * 
 */
@DriverSide
@Unit
public class BGDDriver {
  
  private final DataLoadingService dataLoadingService;
  
  private final GroupCommDriver groupCommDriver;
  
  private final CommunicationGroup allCommGroup; 
  
  private final AtomicInteger slaveContextId = new AtomicInteger(0);
  
  private final IdentifierFactory idFac = new StringIdentifierFactory();
  
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
      .addBroadcast(ControlMessageBroadcaster.class, controlMsgCodec)
      .addBroadcast(ModelBroadcaster.class,modelCodec)
      .addReduce(LossAndGradientReducer.class, lossAndGradientCodec, lossAndGradientReduceFunction)
      .addBroadcast(ModelAndDescentDirectionBroadcaster.class, modelAndDesDirCodec)
      .addReduce(LineSearchEvaluationsReducer.class, lineSearchCodec, lineSearchReduceFunction)
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
          Configuration taskConf = TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, "MasterTask")
              .set(TaskConfiguration.TASK, MasterTask.class)
              .build();
          allCommGroup.setSenderId("MasterTask");
          Configuration cmbConf = allCommGroup.getSenderConfiguration(ControlMessageBroadcaster.class);
          Configuration mbConf = allCommGroup.getSenderConfiguration(ModelBroadcaster.class);
          // TODO: The master task is the receiver for the reduce operator, right? This seems confusing (Markus)
          Configuration lagrConf = allCommGroup.getSenderConfiguration(LossAndGradientReducer.class);
          Configuration mddbConf = allCommGroup.getSenderConfiguration(ModelAndDescentDirectionBroadcaster.class);
          Configuration lserConf = allCommGroup.getSenderConfiguration(LineSearchEvaluationsReducer.class);
          JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder(cmbConf,mbConf,lagrConf,mddbConf,lserConf,taskConf);
          activeContext.submitTask(jcb.build());
        }
        else{
          // TODO: What do we need the special ID for? Can this be arbitrary? Or is this some ID that the group communication layer needs to assign (Markus)
          String slaveId = getSlaveId(activeContext);
          Configuration taskConf = TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, slaveId)
              .set(TaskConfiguration.TASK, MasterTask.class)
              .build();
          Configuration cmbConf = allCommGroup.getReceiverConfiguration(ControlMessageBroadcaster.class,slaveId);
          Configuration mbConf = allCommGroup.getReceiverConfiguration(ModelBroadcaster.class,slaveId);
          Configuration lagrConf = allCommGroup.getReceiverConfiguration(LossAndGradientReducer.class,slaveId);
          Configuration mddbConf = allCommGroup.getReceiverConfiguration(ModelAndDescentDirectionBroadcaster.class,slaveId);
          Configuration lserConf = allCommGroup.getReceiverConfiguration(LineSearchEvaluationsReducer.class,slaveId);
          // TODO: We added Configurations.merge() in Tang 0.4 for this :-) (Markus)
          JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder(cmbConf,mbConf,lagrConf,mddbConf,lserConf,taskConf);
          activeContext.submitTask(jcb.build());
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
  
  public class TaskRunningHandler implements EventHandler<RunningTask> {

    @Override
    public void onNext(RunningTask arg0) {
      
    }
    
  }

}
