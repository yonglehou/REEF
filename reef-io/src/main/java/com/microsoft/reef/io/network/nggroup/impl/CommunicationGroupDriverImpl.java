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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

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
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationSerializer;
import com.microsoft.wake.EStage;

/**
 *
 */
public class CommunicationGroupDriverImpl implements CommunicationGroupDriver {

  private static final Logger LOG = Logger
      .getLogger(CommunicationGroupDriverImpl.class.getName());


  private final Class<? extends Name<String>> groupName;
  private final ConcurrentMap<Class<? extends Name<String>>, OperatorSpec> operatorSpecs = new ConcurrentHashMap<>();
  private final ConcurrentMap<Class<? extends Name<String>>, Topology> topologies = new ConcurrentHashMap<>();
  private final Set<String> taskIds = new HashSet<>();
  private boolean finalised = false;
  private final ConfigurationSerializer confSerializer;
  private final EStage<GroupCommMessage> senderStage;
//  private final MsgAggregator msgAggregator;
  private final String driverId;
  private final int numberOfTasks;

  private final CountingSemaphore allTasksAdded;

  private final Object topologiesLock = new Object();
//  private final Object updatingToplogiesLock = new Object();
  private final Object configLock = new Object();
  private final AtomicBoolean initializing = new AtomicBoolean(true);
//  private final AtomicBoolean updatingTopologies = new AtomicBoolean(false);


  private final Object yetToRunLock = new Object();

  public CommunicationGroupDriverImpl(final Class<? extends Name<String>> groupName,
      final ConfigurationSerializer confSerializer,
      final EStage<GroupCommMessage> senderStage,
      final BroadcastingEventHandler<RunningTask> commGroupRunningTaskHandler,
      final BroadcastingEventHandler<FailedTask> commGroupFailedTaskHandler,
      final BroadcastingEventHandler<FailedEvaluator> commGroupFailedEvaluatorHandler,
      final BroadcastingEventHandler<GroupCommMessage> commGroupMessageHandler,
      final String driverId,
      final int numberOfTasks) {
    super();
    this.groupName = groupName;
    this.numberOfTasks = numberOfTasks;
    this.driverId = driverId;
    this.confSerializer = confSerializer;
//    this.msgAggregator = new MsgAggregator(senderStage);
//    this.pseudoSenderStage = new SyncStage<>(msgAggregator);
    this.senderStage = senderStage;
    this.allTasksAdded = new CountingSemaphore(numberOfTasks, getQualifiedName(),topologiesLock);

    final TopologyRunningTaskHandler topologyRunningTaskHandler = new TopologyRunningTaskHandler(this);
    commGroupRunningTaskHandler.addHandler(topologyRunningTaskHandler);
    final TopologyFailedTaskHandler topologyFailedTaskHandler = new TopologyFailedTaskHandler(this);
    commGroupFailedTaskHandler.addHandler(topologyFailedTaskHandler);
    final TopologyFailedEvaluatorHandler topologyFailedEvaluatorHandler = new TopologyFailedEvaluatorHandler(this);
    commGroupFailedEvaluatorHandler.addHandler(topologyFailedEvaluatorHandler);
    final TopologyMessageHandler topologyMessageHandler = new TopologyMessageHandler(this);
    commGroupMessageHandler.addHandler(topologyMessageHandler);
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

    return this;
  }

  @Override
  public Configuration getConfiguration(final Configuration taskConf) {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final String taskId = taskId(taskConf);
    if(taskIds.contains(taskId)){
      jcb.bindNamedParameter(DriverIdentifier.class, driverId);
      jcb.bindNamedParameter(CommunicationGroupName.class, groupName.getName());
      synchronized (configLock) {
        LOG.info(getQualifiedName() + "Acquired configLock");
        String operWithRunningTask;
        while((operWithRunningTask=operWithRunningTask(taskId))!=null) {
          LOG.info(getQualifiedName() + operWithRunningTask + " thinks "
              + taskId + " is still running. Need to wait for failure");
          try {
            configLock.wait();
          } catch (final InterruptedException e) {
            throw new RuntimeException("InterruptedException while waiting on configLock", e);
          }
        }
        LOG.info(getQualifiedName() + " - No operator thinks " + taskId + " is running. Will fetch configuration now.");
      }
      LOG.info(getQualifiedName() + "Released configLock");
      synchronized (topologiesLock) {
        LOG.info(getQualifiedName() + "Acquired topologiesLock");
        for (final Map.Entry<Class<? extends Name<String>>, OperatorSpec> operSpecEntry : operatorSpecs
            .entrySet()) {
          final Class<? extends Name<String>> operName = operSpecEntry.getKey();
          final Topology topology = topologies.get(operName);
          final JavaConfigurationBuilder jcbInner = Tang.Factory.getTang()
              .newConfigurationBuilder(topology.getConfig(taskId));
          jcbInner.bindNamedParameter(DriverIdentifier.class, driverId);
          jcbInner.bindNamedParameter(OperatorName.class, operName.getName());
          jcb.bindSetEntry(SerializedOperConfigs.class,
              confSerializer.toString(jcbInner.build()));
        }
      }
      LOG.info(getQualifiedName() + "Released topologiesLock");
    }
    return jcb.build();
  }

  /**
   * @param taskId
   * @return
   */
  private String operWithRunningTask(final String taskId) {
    for (final Map.Entry<Class<? extends Name<String>>, OperatorSpec> operSpecEntry : operatorSpecs.entrySet()) {
      final Class<? extends Name<String>> operName = operSpecEntry.getKey();
      final Topology topology = topologies.get(operName);
      if(topology.isRunning(taskId)) {
        return Utils.simpleName(operName);
      }
    }
    return null;
  }

  /**
   * @param taskId
   * @return
   */
  private String operWithNotRunningTask(final String taskId) {
    for (final Map.Entry<Class<? extends Name<String>>, OperatorSpec> operSpecEntry : operatorSpecs.entrySet()) {
      final Class<? extends Name<String>> operName = operSpecEntry.getKey();
      final Topology topology = topologies.get(operName);
      if(!topology.isRunning(taskId)) {
        return Utils.simpleName(operName);
      }
    }
    return null;
  }

  @Override
  public void finalise() {
    finalised = true;
//    msgAggregator.setNumTopologies(topologies.size());
  }

  @Override
  public void addTask(final Configuration partialTaskConf) {
    synchronized (topologiesLock) {
      LOG.info(getQualifiedName() + "Acquired topologiesLock");
      final String taskId = taskId(partialTaskConf);
      for (final Class<? extends Name<String>> operName : operatorSpecs
          .keySet()) {
        final Topology topology = topologies.get(operName);
        topology.addTask(taskId);
      }
      taskIds.add(taskId);
    }
    LOG.info(getQualifiedName() + "Released topologiesLock");
  }

  /**
   * @param id
   */
  public void removeTask(final String id) {
    // TODO Auto-generated method stub

  }


  /**
   * @param id
   */
  public void runTask(final String id) {
    LOG.info(getQualifiedName() + "Task-" + id + " running");
    /*synchronized (updatingToplogiesLock) {
      LOG.info(getQualifiedName() + "Acquired updatingTopologiesLock");
      while(updatingTopologies.get()) {
        LOG.info(getQualifiedName() + "is updating topologies. Will wait for it to finish");
        try {
          updatingToplogiesLock.wait();
        } catch (final InterruptedException e) {
          throw new RuntimeException(
              "InterruptedException while waiting for updatingTopologies lock", e);
        }
        LOG.info(getQualifiedName() + "Finished updating topologies");
      }
      LOG.info(getQualifiedName() + "Released updatingTopologiesLock");
    }*/

    synchronized (topologiesLock) {
      LOG.info(getQualifiedName() + "Acquired topologiesLock");
      for (final Class<? extends Name<String>> operName : operatorSpecs
          .keySet()) {
        final Topology topology = topologies.get(operName);
        topology.setRunning(id);
      }
      allTasksAdded.decrement();
//      msgAggregator.aggregateNSend();
    }
    LOG.info(getQualifiedName() + "Released topologiesLock");
    synchronized (yetToRunLock) {
      LOG.info(getQualifiedName() + "Acquired yetToRunLock");
      yetToRunLock.notifyAll();
    }
    LOG.info(getQualifiedName() + "Released yetToRunLock");
  }

  /**
   * @return
   */
  private String getQualifiedName() {
    return Utils.simpleName(groupName) + " - ";
  }

  /**
   * @param id
   */
  public void failTask(final String id) {
    LOG.info(getQualifiedName() + "Task-" + id + " failed");
    synchronized (yetToRunLock ) {
      LOG.info(getQualifiedName() + "Acquired yetToRunLock");
      String operWithNotRunningTask;
      while((operWithNotRunningTask=operWithNotRunningTask(id))!=null) {
        LOG.info(getQualifiedName() + operWithNotRunningTask + " thinks "
            + id + " is not running to set failure. Need to wait for it run");
        try {
          yetToRunLock.wait();
        } catch (final InterruptedException e) {
          throw new RuntimeException("InterruptedException while waiting on yetToRunLock", e);
        }
      }
      LOG.info(getQualifiedName() + " - No operator thinks " + id
          + " is not running. Can safely set failure.");
    }
    LOG.info(getQualifiedName() + "Released yetToRunLock");
    synchronized (topologiesLock) {
      LOG.info(getQualifiedName() + "Acquired topologiesLock");
      for (final Class<? extends Name<String>> operName : operatorSpecs
          .keySet()) {
        final Topology topology = topologies.get(operName);
        topology.setFailed(id);
      }
      allTasksAdded.increment();
//      msgAggregator.aggregateNSend();
    }
    LOG.info(getQualifiedName() + "Released topologiesLock");
    synchronized (configLock) {
      LOG.info(getQualifiedName() + "Acquired configLock");
      configLock.notifyAll();
    }
    LOG.info(getQualifiedName() + "Released configLock");
  }


  /**
   * @param msg
   */
  public void processMsg(final GroupCommMessage msg) {
    LOG.info(getQualifiedName() + "processing " + msg.getType() + " from "
        + msg.getSrcid());
    synchronized (topologiesLock) {
      LOG.info(getQualifiedName() + "Acquired topologiesLock");
      if(initializing.get() || msg.getType().equals(Type.UpdateTopology)) {
        LOG.info(getQualifiedName() + "waiting for all nodes to run");
        allTasksAdded.await();
        initializing.compareAndSet(true, false);
      }
      final Class<? extends Name<String>> operName = Utils.getClass(msg.getOperatorname());
      topologies.get(operName).processMsg(msg);
    }
    LOG.info(getQualifiedName() + "Released topologiesLock");
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
