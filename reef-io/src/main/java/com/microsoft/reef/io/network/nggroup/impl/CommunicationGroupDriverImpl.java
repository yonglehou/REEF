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

import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.driver.parameters.DriverIdentifier;
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
import com.microsoft.wake.impl.SingleThreadStage;
import com.microsoft.wake.impl.SyncStage;

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
  private final String driverId;
  private final BroadcastingEventHandler<RunningTask> commGroupRunningTaskHandler;
  private final EStage<RunningTask> commGroupRunningTaskStage;
  private final BroadcastingEventHandler<FailedTask> commGroupFailedTaskHandler;
  private final EStage<FailedTask> commGroupFailedTaskStage;
  private final BroadcastingEventHandler<FailedEvaluator> commGroupFailedEvaluatorHandler;
  private final EStage<FailedEvaluator> commGroupFailedEvaluatorStage;
  private final BroadcastingEventHandler<Configuration> commGroupAddTaskHandler;
  private final EStage<Configuration> commGroupAddTaskStage;
  private final CommGroupMessageHandler commGroupMessageHandler;
  private final EStage<GroupCommMessage> commGroupMessageStage;
  private final int numberOfTasks;


  public CommunicationGroupDriverImpl(final Class<? extends Name<String>> groupName,
      final ConfigurationSerializer confSerializer,
      final EStage<GroupCommMessage> senderStage,
      final BroadcastingEventHandler<RunningTask> commGroupRunningTaskHandler,
      final BroadcastingEventHandler<FailedTask> commGroupFailedTaskHandler,
      final BroadcastingEventHandler<FailedEvaluator> commGroupFailedEvaluatorHandler,
      final CommGroupMessageHandler commGroupMessageHandler,
      final String driverId,
      final int numberOfTasks) {
    super();
    this.groupName = groupName;
    this.numberOfTasks = numberOfTasks;
    this.commGroupAddTaskHandler = new BroadcastingEventHandler<>();
    this.commGroupAddTaskStage = new SingleThreadStage<>("CommGroupAddTaskStage", commGroupAddTaskHandler, 10);
    this.commGroupRunningTaskHandler = commGroupRunningTaskHandler;
    this.commGroupRunningTaskStage = new SingleThreadStage<>("CommGroupRunningTaskStage", commGroupRunningTaskHandler, 10);
    this.commGroupFailedTaskHandler = commGroupFailedTaskHandler;
    this.commGroupFailedTaskStage = new SingleThreadStage<>("CommGroupFailedTaskStage", commGroupFailedTaskHandler, 10);
    this.commGroupFailedEvaluatorHandler = commGroupFailedEvaluatorHandler;
    this.commGroupFailedEvaluatorStage = new SingleThreadStage<>("CommGroupFailedEvaluatorStage", commGroupFailedEvaluatorHandler, 10);
    this.commGroupMessageHandler = commGroupMessageHandler;
    this.commGroupMessageStage = new SyncStage<>("CommGroupMessageStage", commGroupMessageHandler);
    this.driverId = driverId;
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
    final Topology topology = new FlatTopology(senderStage, groupName, operatorName, driverId, numberOfTasks);
    topology.setRoot(spec.getSenderId());
    topology.setOperSpec(spec);
    topologies.put(operatorName, topology);
    final TopologyAddTaskHandler topologyAddTaskHandler = new TopologyAddTaskHandler(topology);
    commGroupAddTaskHandler.addHandler(topologyAddTaskHandler);
    final TopologyRunningTaskHandler topologyRunningTaskHandler = new TopologyRunningTaskHandler(topology);
    commGroupRunningTaskHandler.addHandler(topologyRunningTaskHandler);
    final TopologyFailedTaskHandler topologyFailedTaskHandler = new TopologyFailedTaskHandler(topology);
    commGroupFailedTaskHandler.addHandler(topologyFailedTaskHandler);
    final TopologyFailedEvaluatorHandler topologyFailedEvaluatorHandler = new TopologyFailedEvaluatorHandler(topology);
    commGroupFailedEvaluatorHandler.addHandler(topologyFailedEvaluatorHandler);
    final TopologyMessageHandler topologyMessageHandler = new TopologyMessageHandler(topology);
    commGroupMessageHandler.addTopologyMessageHandler(operatorName, topologyMessageHandler);
    return this;
  }

  @Override
  public CommunicationGroupDriver addReduce(
      final Class<? extends Name<String>> operatorName, final ReduceOperatorSpec spec) {
    if(finalised) {
      throw new IllegalStateException("Can't add more operators to a finalised spec");
    }
    operatorSpecs.put(operatorName, spec);
    final Topology topology = new FlatTopology(senderStage, groupName, operatorName, driverId, numberOfTasks);
    topology.setRoot(spec.getReceiverId());
    topology.setOperSpec(spec);
    topologies.put(operatorName, topology);
    final TopologyAddTaskHandler topologyAddTaskHandler = new TopologyAddTaskHandler(topology);
    commGroupAddTaskHandler.addHandler(topologyAddTaskHandler);
    final TopologyRunningTaskHandler topologyRunningTaskHandler = new TopologyRunningTaskHandler(topology);
    commGroupRunningTaskHandler.addHandler(topologyRunningTaskHandler);
    final TopologyFailedTaskHandler topologyFailedTaskHandler = new TopologyFailedTaskHandler(topology);
    commGroupFailedTaskHandler.addHandler(topologyFailedTaskHandler);
    final TopologyFailedEvaluatorHandler topologyFailedEvaluatorHandler = new TopologyFailedEvaluatorHandler(topology);
    commGroupFailedEvaluatorHandler.addHandler(topologyFailedEvaluatorHandler);
    final TopologyMessageHandler topologyMessageHandler = new TopologyMessageHandler(topology);
    commGroupMessageHandler.addTopologyMessageHandler(operatorName, topologyMessageHandler);
    return this;
  }

  @Override
  public Configuration getConfiguration(final Configuration taskConf) {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final String taskId = taskId(taskConf);
    if(taskIds.contains(taskId)){
      jcb.bindNamedParameter(DriverIdentifier.class, driverId);
      jcb.bindNamedParameter(CommunicationGroupName.class, groupName.getName());
      for (final Map.Entry<Class<? extends Name<String>>, OperatorSpec> operSpecEntry : operatorSpecs.entrySet()) {
        final Class<? extends Name<String>> operName = operSpecEntry.getKey();
        final Topology topology = topologies.get(operName);
        final JavaConfigurationBuilder jcbInner = Tang.Factory.getTang().newConfigurationBuilder(topology.getConfig(taskId));
        jcbInner.bindNamedParameter(DriverIdentifier.class, driverId);
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
    commGroupAddTaskStage.onNext(partialTaskConf);
    /*for(final Class<? extends Name<String>> operName : operatorSpecs.keySet()){
      final Topology topology = topologies.get(operName);
      topology.addTask(taskId);
    }*/
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
}
