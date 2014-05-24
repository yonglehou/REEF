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
  private final Set<TaskNode> leaves = new HashSet<>();
  private final EStage<GroupCommMessage> senderStage;
  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;

  /**
   * @param nameServiceAddr
   * @param nameServicePort
   */
  public TaskGraph(
          final EStage<GroupCommMessage> senderStage,
          final Class<? extends Name<String>> groupName,
          final Class<? extends Name<String>> operName
      ) {
    this.senderStage = senderStage;
    this.groupName = groupName;
    this.operName = operName;

  }

  public synchronized void setParent(final String rootId){
    this.root = createTaskNode(rootId);
    for (final TaskNode leaf : leaves) {
      root.addChild(leaf);
      leaf.setParent(root);
    }
  }

  public synchronized void addChild(final String leafId){
    final TaskNode task = createTaskNode(leafId);
    if(root!=null){
      task.setParent(root);
      root.addChild(task);
    }
    leaves.add(task);
  }

  public synchronized void removeLeaf(final String leafId){
    leaves.remove(createTaskNode(leafId));
  }

  /**
   * @param senderId
   * @return
   */
  private TaskNode createTaskNode(final String taskId) {
    return new TaskNodeImpl(taskId);
  }

  /**
   * @param taskId
   */
  public synchronized void setRunning(final String taskId) {
    final TaskNode task = findTask(taskId);
    task.setRunning(true);
    final String rootId = root.taskId();
    if(taskId.equals(rootId)){
      for (final TaskNode child : leaves) {
        if(!child.isRunning()) {
          continue;
        }
        sendSrcAdd(rootId, child.taskId());
      }
    }
    else{
      if(root!=null && root.isRunning()){
        sendSrcAdd(rootId, taskId);
      }
    }
  }

  private void sendSrcAdd(final String parentId, final String childId) {
    senderStage.onNext(Utils.bldGCM(groupName, operName, Type.ChildAdd, childId, parentId, emptyByteArr));
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
      for (final TaskNode child : leaves) {
        if(!child.isRunning()) {
          continue;
        }
        senderStage.onNext(Utils.bldGCM(groupName, operName, Type.ParentDead, taskId, child.taskId(), emptyByteArr));
      }
    }
    else {
      if(root!=null && root.isRunning()) {
        senderStage.onNext(Utils.bldGCM(groupName, operName, Type.ChildDead, taskId, rootId, emptyByteArr));
      }
    }
  }
}
