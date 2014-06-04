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
import java.util.Map;
import java.util.logging.Logger;

import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.reef.io.network.group.operators.GroupCommOperator;
import com.microsoft.reef.io.network.nggroup.api.OperatorSpec;
import com.microsoft.reef.io.network.nggroup.api.TaskNode;
import com.microsoft.reef.io.network.nggroup.api.Topology;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.DataCodec;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.ReduceFunctionParam;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.wake.EStage;

/**
 *
 */
public class FlatTopology implements Topology {

  private static final Logger LOG = Logger.getLogger(FlatTopology.class.getName());


  private final EStage<GroupCommMessage> senderStage;
  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operatorName;
  private final String driverId;
  private String rootId;
  private OperatorSpec operatorSpec;

  private TaskNode root;
  private final Map<String, TaskNode> nodes = new HashMap<>();

  public FlatTopology(
      final EStage<GroupCommMessage> senderStage,
      final Class<? extends Name<String>> groupName,
      final Class<? extends Name<String>> operatorName,
      final String driverId) {
    this.senderStage = senderStage;
    this.groupName = groupName;
    this.operatorName = operatorName;
    this.driverId = driverId;
  }

  @Override
  public void setRoot(final String rootId) {
    this.rootId = rootId;
  }

  @Override
  public void setOperSpec(final OperatorSpec spec) {
    this.operatorSpec = spec;
  }

  @Override
  public Configuration getConfig(final String taskId) {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(DataCodec.class, operatorSpec.getDataCodecClass());
    if(operatorSpec instanceof BroadcastOperatorSpec){
      final BroadcastOperatorSpec broadcastOperatorSpec = (BroadcastOperatorSpec) operatorSpec;
      if(taskId.equals(broadcastOperatorSpec.getSenderId())){
        jcb.bindImplementation(GroupCommOperator.class, BroadcastSender.class);
      }
      else{
        jcb.bindImplementation(GroupCommOperator.class, BroadcastReceiver.class);
      }
    }
    if(operatorSpec instanceof ReduceOperatorSpec){
      final ReduceOperatorSpec reduceOperatorSpec = (ReduceOperatorSpec) operatorSpec;
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
  public void addTask(final String taskId) {
    if(taskId.equals(rootId)) {
      setRootNode(taskId);
    }
    else {
      addChild(taskId);
    }
  }

  /**
   * @param taskId
   */
  private synchronized void addChild(final String taskId) {
    LOG.info(getQualifiedName() + "Adding leaf " + taskId);
    final TaskNode node = new TaskNodeImpl(senderStage, groupName, operatorName, taskId, driverId);
    final TaskNode leaf = node;
    if(root!=null) {
      LOG.info(getQualifiedName() + "Setting " + rootId + " as parent of " + taskId);
      leaf.setParent(root);
      LOG.info(getQualifiedName() + "Adding " + taskId + " as leaf of " + rootId);
      root.addChild(leaf);
    }
    nodes.put(taskId,leaf);
  }

  private synchronized void setRootNode(final String rootId){
    LOG.info(getQualifiedName() + "Setting " + rootId + " as root");
    final TaskNode node = new TaskNodeImpl(senderStage, groupName, operatorName, rootId, driverId);
    this.root = node;
    for(final Map.Entry<String, TaskNode> nodeEntry : nodes.entrySet()) {
      final String id = nodeEntry.getKey();

      final TaskNode leaf = nodeEntry.getValue();

      LOG.info(getQualifiedName() + "Adding " + id + " as leaf of " + rootId);
      root.addChild(leaf);
      LOG.info(getQualifiedName() + "Setting " + rootId + " as parent of " + id);
      leaf.setParent(root);
    }
    nodes.put(rootId, root);
  }


  @Override
  public void setFailed(final String id) {
    nodes.get(id).setFailed();
  }

  @Override
  public void setRunning(final String id) {
    nodes.get(id).setRunning();
  }

  @Override
  public void processMsg(final GroupCommMessage msg) {
    final String id = msg.getSrcid();
    nodes.get(id).processMsg(msg);
  }

  /**
   * @return
   */
  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operatorName) + " - ";
  }

  @Override
  public void handle(final RunningTask runningTask) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void handle(final FailedTask failedTask) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void handle(final GroupCommMessage gcm) {
    throw new UnsupportedOperationException();
  }
}
