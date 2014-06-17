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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import com.microsoft.reef.io.network.group.operators.GroupCommOperator;
import com.microsoft.reef.io.network.nggroup.api.GroupChanges;
import com.microsoft.reef.io.network.nggroup.api.OperatorSpec;
import com.microsoft.reef.io.network.nggroup.api.TaskNode;
import com.microsoft.reef.io.network.nggroup.api.Topology;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.DataCodec;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.ReduceFunctionParam;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.SingleThreadStage;

/**
 *
 */
public class FlatTopology implements Topology {

  private static final Logger LOG = Logger.getLogger(FlatTopology.class.getName());


  private final EStage<GroupCommMessage> senderStage;
  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String driverId;
  private String rootId;
  private OperatorSpec operatorSpec;

  private TaskNode root;
  private final ConcurrentMap<String, TaskNode> nodes = new ConcurrentHashMap<>();

  private final CountingSemaphore allTasksAdded;

  private final AtomicBoolean firstTime = new AtomicBoolean(false);

  public FlatTopology(
      final EStage<GroupCommMessage> senderStage,
      final Class<? extends Name<String>> groupName,
      final Class<? extends Name<String>> operatorName,
      final String driverId,
      final int numberOfTasks) {
    this.senderStage = senderStage;
    this.groupName = groupName;
    this.operName = operatorName;
    this.driverId = driverId;
    this.allTasksAdded = new CountingSemaphore(numberOfTasks);
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
  public void removeTask(final String taskId) {
    if(!nodes.containsKey(taskId)) {
      LOG.warning("Trying to remove a non-existent node in the task graph");
      return;
    }
    if(taskId.equals(rootId)) {
      unsetRootNode(taskId);
    }
    else {
      removeChild(taskId);
    }
  }

  @Override
  public void addTask(final String taskId) {
    if(nodes.containsKey(taskId)) {
      LOG.warning("Got a request to add a task that is already in the graph");
      LOG.warning("We need to block this request till the delete finishes");
    }
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
  private void addChild(final String taskId) {
    synchronized (nodes) {
      LOG.info(getQualifiedName() + "Adding leaf " + taskId);
      final TaskNode node = new TaskNodeImpl(senderStage, groupName, operName,
          taskId, driverId);
      final TaskNode leaf = node;
      if (root != null) {
        LOG.info(getQualifiedName() + "Setting " + rootId + " as parent of "
            + taskId);
        leaf.setParent(root);
        LOG.info(getQualifiedName() + "Adding " + taskId + " as leaf of "
            + rootId);
        root.addChild(leaf);
      }
      nodes.put(taskId, leaf);
    }
  }

  /**
   * @param taskId
   */
  private void removeChild(final String taskId) {
    synchronized (nodes) {
      LOG.info(getQualifiedName() + "Removing leaf " + taskId);
      if (root != null) {
        LOG.info(getQualifiedName() + "Removing " + taskId + " as leaf of "
            + rootId);
        root.removeChild(nodes.get(taskId));
      }
      nodes.remove(taskId);
    }
  }

  private void setRootNode(final String rootId){
    synchronized (nodes) {
      LOG.info(getQualifiedName() + "Setting " + rootId + " as root");
      final TaskNode node = new TaskNodeImpl(senderStage, groupName, operName, rootId, driverId);
      this.root = node;


      for (final Map.Entry<String, TaskNode> nodeEntry : nodes.entrySet()) {
        final String id = nodeEntry.getKey();

        final TaskNode leaf = nodeEntry.getValue();

        LOG.info(getQualifiedName() + "Adding " + id + " as leaf of " + rootId);
        root.addChild(leaf);
        LOG.info(getQualifiedName() + "Setting " + rootId + " as parent of "
            + id);
        leaf.setParent(root);
      }

      nodes.put(rootId, root);
    }
  }

  /**
   * @param taskId
   */
  private void unsetRootNode(final String taskId) {
    synchronized (nodes) {
      LOG.info(getQualifiedName() + "Unsetting " + rootId + " as root");
      nodes.remove(rootId);

      for (final Map.Entry<String, TaskNode> nodeEntry : nodes.entrySet()) {
        final String id = nodeEntry.getKey();

        final TaskNode leaf = nodeEntry.getValue();

        LOG.info(getQualifiedName() + "Setting parent to null for " + id);
        leaf.setParent(null);
      }
    }
  }

  @Override
  public void setFailed(final String id) {
    LOG.info(getQualifiedName() + "Task-" + id + " failed");
    allTasksAdded.increment();
    final TaskNode taskNode = nodes.get(id);
    if(taskNode!=null) {
      taskNode.setFailed();
    } else {
      LOG.warning(getQualifiedName() + id + " does not exist");
    }
  }

  @Override
  public void setRunning(final String id) {
    LOG.info(getQualifiedName() + "Task-" + id + " is running");
    final TaskNode taskNode = nodes.get(id);
    if(taskNode!=null) {
      taskNode.setRunning();
    } else {
      LOG.warning(getQualifiedName() + id + " does not exist");
    }
    allTasksAdded.decrement();
  }

  @Override
  public void processMsg(final GroupCommMessage msg) {
    if(firstTime.compareAndSet(false, true)) {
      LOG.info(getQualifiedName() + "waiting for all nodes to run");
      allTasksAdded.await();
    }
    LOG.info(getQualifiedName() + "processing " + msg.getType() + " from " + msg.getSrcid());
    if(msg.getType().equals(Type.TopologyChanges)) {
      final String dstId = msg.getSrcid();
      final GroupChanges changes = new GroupChangesImpl(false);
      synchronized (nodes) {
        LOG.info(getQualifiedName() + "Checking which nodes need to be updated");
        for (final TaskNode node : nodes.values()) {
          if(node.isRunning()) {
            LOG.info(getQualifiedName() + node.taskId() + " is running");
            if(node.hasChanges()) {
              LOG.info(getQualifiedName() + node.taskId() + " has changes.");
              changes.setChanges(true);
              break;
            }
            else {
              LOG.info(getQualifiedName() + node.taskId() + " has no changes. Skipping");
            }
          }
          else {
            LOG.info(getQualifiedName() + node.taskId() + " is not running. Skipping");
          }
        }
      }
      final Codec<GroupChanges> changesCodec = new GroupChangesCodec();
      LOG.info("Sending GroupChanges to " + dstId);
      senderStage.onNext(Utils.bldGCM(groupName, operName, Type.TopologyChanges, driverId, dstId, changesCodec.encode(changes)));
      return;
    }
    if(msg.getType().equals(Type.UpdateTopology)) {
      allTasksAdded.await();
      final String dstId = msg.getSrcid();
      LOG.info(getQualifiedName() + "Creating NodeTopologyUpdateWaitStage to wait on nodes to be updated");
      //This stage only waits for receiving TopologySetup
      //Sending UpdateTopology to the tasks is left to NodeStatusImpl as part
      //of processMsg
      final EStage<List<TaskNode>> nodeTopologyUpdateWaitStage = new SingleThreadStage<>("NodeTopologyUpdateWaitStage", new EventHandler<List<TaskNode>>() {

        @Override
            public void onNext(final List<TaskNode> nodes) {
              LOG.info(getQualifiedName()
                  + "NodeTopologyUpdateWaitStage received " + nodes.size()
                  + " to be updated nodes to waitfor");
              for (final TaskNode node : nodes) {
                LOG.info(getQualifiedName()
                    + "NodeTopologyUpdateWaitStage waiting for "
                    + node.taskId() + " to receive TopologySetup");
                node.waitForTopologySetup();
              }
              LOG.info(getQualifiedName()
                  + "NodeTopologyUpdateWaitStage All to be updated nodes " +
                  "have received TopologySetup. Sending TopologyUpdated");
              senderStage.onNext(Utils.bldGCM(groupName, operName,
                  Type.TopologyUpdated, driverId, dstId, new byte[0]));
            }
      }, nodes.size());
      final List<TaskNode> toBeUpdatedNodes = new ArrayList<>(nodes.size());
      synchronized (nodes) {
        LOG.info(getQualifiedName() + "Checking which nodes need to be updated");
        for (final TaskNode node : nodes.values()) {
          if(node.isRunning()) {
            LOG.info(getQualifiedName() + node.taskId() + " is running");
            if(node.hasChanges() && node.resetTopologySetupSent()) {
              LOG.info(getQualifiedName() + node.taskId() + " has changes. " +
              		"Reset the TopologySetupSent flag & add to list");
              toBeUpdatedNodes.add(node);
            }
            else {
              LOG.info(getQualifiedName() + node.taskId() + " has no changes. Skipping");
            }
          }
          else {
            LOG.info(getQualifiedName() + node.taskId() + " is not running. Skipping");
          }
        }
      }
      nodeTopologyUpdateWaitStage.onNext(toBeUpdatedNodes);
      for (final TaskNode node : toBeUpdatedNodes) {
        LOG.info(getQualifiedName() + node.taskId() + " process UpdateTopology msg since you have changes");
        node.processMsg(msg);
        //The stage will wait for all nodes to acquire topoLock
        //and send TopologySetup msg. Then it will send TopologyUpdated
        //msg. However, any local topology changes are not in effect
        //till driver sends TopologySetup once statusMap is emptied
        //The operations in the tasks that have topology changes will
        //wait for this. However other tasks that do not have any changes
        //will continue their regular operation
      }
      //Handling of UpdateTopology msg done. Return
      return;
    }
    final String id = msg.getSrcid();
    nodes.get(id).processMsg(msg);
  }

  /**
   * @return
   */
  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName) + " - ";
  }
}
