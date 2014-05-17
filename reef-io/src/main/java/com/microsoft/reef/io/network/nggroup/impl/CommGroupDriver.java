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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.GroupCommOperator;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupDriver;
import com.microsoft.reef.io.network.nggroup.api.OperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.CommunicationGroupName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.DataCodec;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.OperatorName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.ReduceFunction;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.SerializedOperConfigs;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.AvroConfigurationSerializer;
import com.microsoft.tang.formats.ConfigurationSerializer;

/**
 * 
 */
public class CommGroupDriver implements CommunicationGroupDriver {
  
  private final Class<? extends Name<String>> groupName;
  private final Map<Class<? extends Name<String>>, OperatorSpec> operatorSpecs;
  private final Set<String> taskIds = new HashSet<>();
  private boolean finalised = false;
  private final ConfigurationSerializer confSerializer;

  public CommGroupDriver(Class<? extends Name<String>> groupName, ConfigurationSerializer confSerializer) {
    super();
    this.groupName = groupName;
    this.operatorSpecs = new HashMap<>();
    this.confSerializer = confSerializer;
  }

  @Override
  public CommunicationGroupDriver addBroadcast(
      Class<? extends Name<String>> operatorName, OperatorSpec spec) {
    if(finalised)
      throw new IllegalStateException("Can't add more operators to a finalised spec");
    operatorSpecs.put(operatorName, spec);
    return this;
  }

  @Override
  public CommunicationGroupDriver addReduce(
      Class<? extends Name<String>> operatorName, ReduceOperatorSpec spec) {
    if(finalised)
      throw new IllegalStateException("Can't add more operators to a finalised spec");
    operatorSpecs.put(operatorName, spec);
    return this;
  }

  @Override
  public Configuration getConfiguration(Configuration taskConf) {
    JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    String taskId = taskId(taskConf);
    if(taskIds.contains(taskId)){
      jcb.bindNamedParameter(CommunicationGroupName.class, groupName);
      for (Map.Entry<Class<? extends Name<String>>, OperatorSpec> operSpecEntry : operatorSpecs.entrySet()) {
        JavaConfigurationBuilder jcbInner = Tang.Factory.getTang().newConfigurationBuilder();
        jcbInner.bindNamedParameter(OperatorName.class, operSpecEntry.getKey());
        final OperatorSpec operSpec = operSpecEntry.getValue();
        jcbInner.bindNamedParameter(DataCodec.class, operSpec.getDataCodecClass());
        if(operSpec instanceof BroadcastOperatorSpec){
          BroadcastOperatorSpec broadcastOperatorSpec = (BroadcastOperatorSpec) operSpec;
          if(taskId.equals(broadcastOperatorSpec.getSenderId())){
            jcbInner.bindImplementation(GroupCommOperator.class, BroadcastSender.class);
          }
          else{
            jcbInner.bindImplementation(GroupCommOperator.class, BroadcastReceiver.class);
          }
        }
        if(operSpec instanceof ReduceOperatorSpec){
          ReduceOperatorSpec reduceOperatorSpec = (ReduceOperatorSpec) operSpec;
          jcbInner.bindNamedParameter(ReduceFunction.class, reduceOperatorSpec.getRedFuncClass());
          if(taskId.equals(reduceOperatorSpec.getReceiverId())){
            jcbInner.bindImplementation(GroupCommOperator.class, ReduceReceiver.class);
          }
          else{
            jcbInner.bindImplementation(GroupCommOperator.class, ReduceSender.class);
          }
        }
        jcb.bindSetEntry(SerializedOperConfigs.class, confSerializer.toString(jcbInner.build()));
      }
    }
    return jcb.build();
  }

  @Override
  public void finalise() {
    finalised = true;
  }

  @Override
  public void addTask(Configuration partialTaskConf) {
    String taskId = taskId(partialTaskConf);
    taskIds.add(taskId);
  }
  
  private String taskId(Configuration partialTaskConf){
    try{
      Injector injector = Tang.Factory.getTang().newInjector(partialTaskConf);
      return injector.getNamedInstance(TaskConfigurationOptions.Identifier.class);
    } catch(InjectionException e){
      throw new RuntimeException("Unable to find task identifier", e);
    }
  }

}
