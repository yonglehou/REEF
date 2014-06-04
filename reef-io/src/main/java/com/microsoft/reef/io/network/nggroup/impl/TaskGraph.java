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
import java.util.Set;
import java.util.logging.Logger;

import com.microsoft.reef.io.network.nggroup.api.TaskNode;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.tang.annotations.Name;
import com.microsoft.wake.EStage;

/**
 *
 */
public class TaskGraph{
  /**
   *
   */
  private static final byte[] emptyByteArr = new byte[0];
  private static final Logger LOG = Logger.getLogger(TaskGraph.class.getName());
  private TaskNode root;
  private String rootId;
  private final Set<TaskNode> leaves = new HashSet<>();
  private final EStage<GroupCommMessage> senderStage;
  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String driverId;

  /**
   * @param driverId
   * @param nameServiceAddr
   * @param nameServicePort
   */
  public TaskGraph(
          final EStage<GroupCommMessage> senderStage,
          final Class<? extends Name<String>> groupName,
          final Class<? extends Name<String>> operName,
          final String driverId
      ) {
    LOG.info(getQualifiedName() + "Creating task graph");
    this.senderStage = senderStage;
    this.groupName = groupName;
    this.operName = operName;
    this.driverId = driverId;

  }

  public void setRoot(final String rootId) {
    this.rootId= rootId;
  }

  public synchronized void setParent(final String rootId){
    LOG.info(getQualifiedName() + "Setting " + rootId + " as root");
    this.root = createTaskNode(rootId);
    for (final TaskNode leaf : leaves) {
      LOG.info(getQualifiedName() + "Adding " + leaf.taskId() + " as child of " + rootId);
      root.addChild(leaf);
      LOG.info(getQualifiedName() + "Setting " + rootId + " as parent of " + leaf.taskId());
      leaf.setParent(root);
    }
  }

  public synchronized void addTask(final String leafId){
    if(leafId.equals(rootId)) {
      setParent(leafId);
    } else {
      addChild(leafId);
    }
  }

  private void addChild(final String leafId) {
    LOG.info(getQualifiedName() + "Adding leaf " + leafId);
    final TaskNode task = createTaskNode(leafId);
    if(root!=null){
      LOG.info(getQualifiedName() + "Setting " + root.taskId() + " as parent of " + leafId);
      task.setParent(root);
      LOG.info(getQualifiedName() + "Adding " + leafId + " as child of " + root.taskId());
      root.addChild(task);
    }
    leaves.add(task);
  }

  /*public synchronized void removeLeaf(final String leafId){
    LOG.info(getQualifiedName() + "Removing leaf " + leafId);
    leaves.remove(createTaskNode(leafId));
  }*/

  /**
   * @param senderId
   * @return
   */
  private TaskNode createTaskNode(final String taskId) {
    return new TaskNodeImpl(senderStage, groupName, operName, taskId, driverId);
  }

  /**
   * @param taskId
   */
  public synchronized void setRunning(final String taskId) {
    final TaskNode task = findTask(taskId);
    assert(task!=null);
    task.setRunning(true);
    final String rootId = root.taskId();
    if(taskId.equals(rootId)){
      LOG.info(getQualifiedName() + taskId + " - Running task is root");
      for (final TaskNode child : leaves) {
        if(!child.isRunning()) {
          LOG.info(getQualifiedName() + child.taskId() + " is not running yet. skipping src add send");
          continue;
        }
        sendSrcAdd(rootId, child.taskId());
      }
    }
    else{
      LOG.info(getQualifiedName() + taskId + " - Running task is a leaf");
      if(root!=null && root.isRunning()){
        sendSrcAdd(rootId, taskId);
      }
    }
  }

  private void sendSrcAdd(final String parentId, final String childId) {
    LOG.info(getQualifiedName() + "Sending ChildAdd from " + childId + " to " + parentId);
    senderStage.onNext(Utils.bldGCM(groupName, operName, Type.ChildAdd, childId, parentId, emptyByteArr));
    LOG.info(getQualifiedName() + "Sending ParentAdd from " + parentId + " to " + childId);
    senderStage.onNext(Utils.bldGCM(groupName, operName, Type.ParentAdd, parentId, childId, emptyByteArr));
  }

  /**
   * @param taskId
   * @return
   */
  private synchronized TaskNode findTask(final String taskId) {
    if(root.taskId().equals(taskId)) {
      return root;
    }
    for(final TaskNode task : leaves){
      if(task.taskId().equals(taskId)) {
        return task;
      }
    }
    return null;
  }

  /**
   * @param taskId
   */
  public synchronized void setFailed(final String taskId) {
    final TaskNode task = findTask(taskId);
    task.setRunning(false);
    final String rootId = root.taskId();
    if(taskId.equals(rootId)){
      LOG.info(getQualifiedName() + "Root node " + taskId + " failed");
      for (final TaskNode child : leaves) {
        if(!child.isRunning()) {
          continue;
        }
        child.removeActiveNeighbor(taskId);
        LOG.info(getQualifiedName() + "Sending ParentDead msg from " + taskId + " to " + child.taskId());
        senderStage.onNext(Utils.bldGCM(groupName, operName, Type.ParentDead, taskId, child.taskId(), emptyByteArr));
      }
    }
    else {
      LOG.info(getQualifiedName() + "Leaf node " + taskId + " failed");
      if(root!=null && root.isRunning()) {
        root.removeActiveNeighbor(taskId);
        LOG.info(getQualifiedName() + "Sending ChildDead msg from " + taskId + " to " + rootId);
        senderStage.onNext(Utils.bldGCM(groupName, operName, Type.ChildDead, taskId, rootId, emptyByteArr));
      }
    }
  }

  /**
   * @param gcm
   */
  public void handle(final GroupCommMessage gcm) {
    final String nodeId = gcm.getSrcid();
    final TaskNode node = findTask(nodeId);
    node.handle(gcm);
  }


  /**
   * @return
   */
  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName) + " - ";
  }
}
