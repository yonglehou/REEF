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

import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.io.network.Message;
import com.microsoft.reef.io.network.group.impl.GCMCodec;
import com.microsoft.reef.io.network.impl.MessagingTransportFactory;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupDriver;
import com.microsoft.reef.io.network.nggroup.api.OperatorSpec;
import com.microsoft.reef.io.network.nggroup.api.Topology;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.CommunicationGroupName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.OperatorName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.SerializedOperConfigs;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationSerializer;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.impl.LoggingEventHandler;

/**
 * 
 */
public class CommGroupDriver implements CommunicationGroupDriver {
  
  private final Class<? extends Name<String>> groupName;
  private final Map<Class<? extends Name<String>>, OperatorSpec> operatorSpecs;
  private final Map<Class<? extends Name<String>>, Topology> topologies;
  private final Set<String> taskIds = new HashSet<>();
  private boolean finalised = false;
  private final ConfigurationSerializer confSerializer;
  private final NetworkService<GroupCommMessage> netService;
  private final IdentifierFactory idFac = new StringIdentifierFactory();

  public CommGroupDriver(Class<? extends Name<String>> groupName,
      ConfigurationSerializer confSerializer, String nameServiceAddr,
      int nameServicePort) {
    super();
    this.groupName = groupName;
    this.operatorSpecs = new HashMap<>();
    this.topologies = new HashMap<>();
    this.confSerializer = confSerializer;
    netService = new NetworkService<>(idFac, 0, nameServiceAddr,
        nameServicePort, 5, 100, new GCMCodec(),
        new MessagingTransportFactory(),
        new LoggingEventHandler<Message<GroupCommMessage>>(),
        new LoggingEventHandler<Exception>());
  }

  @Override
  public CommunicationGroupDriver addBroadcast(
      Class<? extends Name<String>> operatorName, BroadcastOperatorSpec spec) {
    if(finalised)
      throw new IllegalStateException("Can't add more operators to a finalised spec");
    operatorSpecs.put(operatorName, spec);
    Topology topology = new FlatTopology(netService, groupName, operatorName);
    topology.setRoot(spec.getSenderId());
    topology.setOperSpec(spec);
    topologies.put(operatorName, topology);
    return this;
  }

  @Override
  public CommunicationGroupDriver addReduce(
      Class<? extends Name<String>> operatorName, ReduceOperatorSpec spec) {
    if(finalised)
      throw new IllegalStateException("Can't add more operators to a finalised spec");
    operatorSpecs.put(operatorName, spec);
    Topology topology = new FlatTopology(netService, groupName, operatorName);
    topology.setRoot(spec.getReceiverId());
    topology.setOperSpec(spec);
    topologies.put(operatorName, topology);
    return this;
  }

  @Override
  public Configuration getConfiguration(Configuration taskConf) {
    JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    String taskId = taskId(taskConf);
    if(taskIds.contains(taskId)){
      jcb.bindNamedParameter(CommunicationGroupName.class, groupName);
      for (Map.Entry<Class<? extends Name<String>>, OperatorSpec> operSpecEntry : operatorSpecs.entrySet()) {
        final Class<? extends Name<String>> operName = operSpecEntry.getKey();
        Topology topology = topologies.get(operName);
        JavaConfigurationBuilder jcbInner = Tang.Factory.getTang().newConfigurationBuilder(topology.getConfig(taskId));
        jcbInner.bindNamedParameter(OperatorName.class, operName);
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
    for(Class<? extends Name<String>> operName : operatorSpecs.keySet()){
      Topology topology = topologies.get(operName);
      topology.addTask(taskId);
    }
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

  @Override
  public void handle(RunningTask runningTask) {
    for(Map.Entry<Class<? extends Name<String>>, Topology> topEntry : topologies.entrySet()){
      Topology topology = topEntry.getValue();
      topology.handle(runningTask);
    }
  }

  @Override
  public void handle(FailedTask failedTask) {
    for(Map.Entry<Class<? extends Name<String>>, Topology> topEntry : topologies.entrySet()){
      Topology topology = topEntry.getValue();
      topology.handle(failedTask);
    }
  }

}
