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
import com.microsoft.wake.EStage;
import com.microsoft.wake.IdentifierFactory;

/**
 *
 */
public class CommunicationGroupDriverImpl implements CommunicationGroupDriver {

  private final Class<? extends Name<String>> groupName;
  private final Map<Class<? extends Name<String>>, OperatorSpec> operatorSpecs;
  private final Map<Class<? extends Name<String>>, Topology> topologies;
  private final Set<String> taskIds = new HashSet<>();
  private boolean finalised = false;
  private final ConfigurationSerializer confSerializer;
  private final IdentifierFactory idFac = new StringIdentifierFactory();
  private final EStage<GroupCommMessage> senderStage;

  public CommunicationGroupDriverImpl(final Class<? extends Name<String>> groupName,
      final ConfigurationSerializer confSerializer, final EStage<GroupCommMessage> senderStage) {
    super();
    this.groupName = groupName;
    this.operatorSpecs = new HashMap<>();
    this.topologies = new HashMap<>();
    this.confSerializer = confSerializer;
    this.senderStage = senderStage;
  }

  @Override
  public CommunicationGroupDriver addBroadcast(
      final Class<? extends Name<String>> operatorName, final BroadcastOperatorSpec spec) {
    if(finalised) {
      throw new IllegalStateException("Can't add more operators to a finalised spec");
    }
    operatorSpecs.put(operatorName, spec);
    final Topology topology = new FlatTopology(senderStage, groupName, operatorName);
    topology.setRoot(spec.getSenderId());
    topology.setOperSpec(spec);
    topologies.put(operatorName, topology);
    return this;
  }

  @Override
  public CommunicationGroupDriver addReduce(
      final Class<? extends Name<String>> operatorName, final ReduceOperatorSpec spec) {
    if(finalised) {
      throw new IllegalStateException("Can't add more operators to a finalised spec");
    }
    operatorSpecs.put(operatorName, spec);
    final Topology topology = new FlatTopology(senderStage, groupName, operatorName);
    topology.setRoot(spec.getReceiverId());
    topology.setOperSpec(spec);
    topologies.put(operatorName, topology);
    return this;
  }

  @Override
  public Configuration getConfiguration(final Configuration taskConf) {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final String taskId = taskId(taskConf);
    if(taskIds.contains(taskId)){
      jcb.bindNamedParameter(CommunicationGroupName.class, groupName.getName());
      for (final Map.Entry<Class<? extends Name<String>>, OperatorSpec> operSpecEntry : operatorSpecs.entrySet()) {
        final Class<? extends Name<String>> operName = operSpecEntry.getKey();
        final Topology topology = topologies.get(operName);
        final JavaConfigurationBuilder jcbInner = Tang.Factory.getTang().newConfigurationBuilder(topology.getConfig(taskId));
        jcbInner.bindNamedParameter(OperatorName.class, operName.getName());
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
  public void addTask(final Configuration partialTaskConf) {
    final String taskId = taskId(partialTaskConf);
    for(final Class<? extends Name<String>> operName : operatorSpecs.keySet()){
      final Topology topology = topologies.get(operName);
      topology.addTask(taskId);
    }
    taskIds.add(taskId);
  }

  private String taskId(final Configuration partialTaskConf){
    try{
      final Injector injector = Tang.Factory.getTang().newInjector(partialTaskConf);
      return injector.getNamedInstance(TaskConfigurationOptions.Identifier.class);
    } catch(final InjectionException e){
      throw new RuntimeException("Unable to find task identifier", e);
    }
  }

  @Override
  public void handle(final RunningTask runningTask) {
    for(final Map.Entry<Class<? extends Name<String>>, Topology> topEntry : topologies.entrySet()){
      final Topology topology = topEntry.getValue();
      topology.handle(runningTask);
    }
  }

  @Override
  public void handle(final FailedTask failedTask) {
    for(final Map.Entry<Class<? extends Name<String>>, Topology> topEntry : topologies.entrySet()){
      final Topology topology = topEntry.getValue();
      topology.handle(failedTask);
    }
  }

}
