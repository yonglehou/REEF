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
package com.microsoft.reef.io.network.nggroup.impl;

import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.reef.io.network.group.operators.GroupCommOperator;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.nggroup.api.OperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.DataCodec;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.ReduceFunctionParam;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;

/**
 * 
 */
public class FlatTopology implements com.microsoft.reef.io.network.nggroup.api.Topology{
  
  private TaskGraph taskGraph;
  private OperatorSpec operatorSpec;
  

  public FlatTopology(
      NetworkService<GroupCommMessage> netService, 
      Class<? extends Name<String>> groupName, 
      Class<? extends Name<String>> operName) {
    super();
    this.taskGraph = new TaskGraph(netService,groupName,operName);
  }

  @Override
  public void setRoot(String senderId) {
    taskGraph.setParent(senderId);
  }

  @Override
  public void setOperSpec(OperatorSpec spec) {
    operatorSpec = spec;
  }

  @Override
  public Configuration getConfig(String taskId) {
    JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(DataCodec.class, operatorSpec.getDataCodecClass());
    if(operatorSpec instanceof BroadcastOperatorSpec){
      BroadcastOperatorSpec broadcastOperatorSpec = (BroadcastOperatorSpec) operatorSpec;
      if(taskId.equals(broadcastOperatorSpec.getSenderId())){
        jcb.bindImplementation(GroupCommOperator.class, BroadcastSender.class);
      }
      else{
        jcb.bindImplementation(GroupCommOperator.class, BroadcastReceiver.class);
      }
    }
    if(operatorSpec instanceof ReduceOperatorSpec){
      ReduceOperatorSpec reduceOperatorSpec = (ReduceOperatorSpec) operatorSpec;
      jcb.bindNamedParameter(ReduceFunctionParam.class, reduceOperatorSpec.getRedFuncClass());
      if(taskId.equals(reduceOperatorSpec.getReceiverId())){
        jcb.bindImplementation(GroupCommOperator.class, ReduceReceiver.class);
      }
      else{
        jcb.bindImplementation(GroupCommOperator.class, ReduceSender.class);
      }
    }
    return jcb.build();
  }

  @Override
  public void addTask(String taskId) {
    taskGraph.addChild(taskId);
  }

  @Override
  public void handle(RunningTask runningTask) {
    String taskId = runningTask.getId();
    taskGraph.setRunning(taskId);
  }

  @Override
  public void handle(FailedTask failedTask) {
    String taskId = failedTask.getId();
    taskGraph.setFailed(taskId);
  }

}
