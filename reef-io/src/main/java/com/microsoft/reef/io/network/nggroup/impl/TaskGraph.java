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
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.Connection;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.nggroup.api.TaskNode;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.tang.annotations.Name;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.impl.ThreadPoolStage;

/**
 * 
 */
public class TaskGraph{
  private static final Logger LOG = Logger.getLogger(TaskGraph.class.getName());
  private TaskNode root;
  private final Set<TaskNode> leaves = new HashSet<>();
  private final NetworkService<GroupCommMessage> netService;
  private final ThreadPoolStage<GroupCommMessage> senderStage;
  private final IdentifierFactory idFac = new StringIdentifierFactory();
  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  
  /**
   * @param nameServiceAddr
   * @param nameServicePort
   */
  public TaskGraph(
          NetworkService<GroupCommMessage> netService, 
          Class<? extends Name<String>> groupName, 
          Class<? extends Name<String>> operName
      ) {
    this.netService = netService;
    this.groupName = groupName;
    this.operName = operName;
    this.senderStage = new ThreadPoolStage<>(
        "SrcCtrlMsgSender", new EventHandler<GroupCommMessage>() {
      @Override
      public void onNext(GroupCommMessage srcCtrlMsg) {

        final Identifier id = TaskGraph.this.idFac.getNewInstance(srcCtrlMsg.getDestid());

        final Connection<GroupCommMessage> link = TaskGraph.this.netService.newConnection(id);
        try {
          link.open();
          LOG.log(Level.FINEST, "Sending source ctrl msg {0} for {1} to {2}",
              new Object[] { srcCtrlMsg.getType(), srcCtrlMsg.getSrcid(), id });
          link.write(srcCtrlMsg);
        } catch (final NetworkException e) {
          LOG.log(Level.WARNING, "Unable to send ctrl task msg to parent " + id, e);
          throw new RuntimeException("Unable to send ctrl task msg to parent " + id, e);
        }
      }
    }, 5);
  }

  public synchronized void setParent(String rootId){
    this.root = createTaskNode(rootId);
    for (TaskNode leaf : leaves) {
      root.addChild(leaf);
      leaf.setParent(root);
    }
  }
  
  public synchronized void addChild(String leafId){
    final TaskNode task = createTaskNode(leafId);
    if(root!=null){
      task.setParent(root);
      root.addChild(task);
    }
    leaves.add(task);
  }
  
  public synchronized void removeLeaf(String leafId){
    leaves.remove(createTaskNode(leafId));
  }
  
  /**
   * @param senderId
   * @return
   */
  private TaskNode createTaskNode(String taskId) {
    return new TaskNodeImpl(taskId);
  }

  /**
   * @param taskId
   */
  public synchronized void setRunning(String taskId) {
    TaskNode task = findTask(taskId);
    task.setRunning(true);
    final String rootId = root.taskId();
    if(taskId.equals(rootId)){
      for (TaskNode child : leaves) {
        if(!child.isRunning())
          continue;
        sendSrcAdd(rootId, child.taskId());
      }
    }
    else{
      if(root!=null && root.isRunning()){
        sendSrcAdd(rootId, taskId);
      }
    }
  }

  private void sendSrcAdd(final String parentId, String childId) {
    senderStage.onNext(Utils.bldGCM(groupName, operName, Type.ChildAdd, childId, parentId, new byte[0]));
    senderStage.onNext(Utils.bldGCM(groupName, operName, Type.ParentAdd, parentId, childId, new byte[0]));
  }

  /**
   * @param taskId
   * @return
   */
  private synchronized TaskNode findTask(String taskId) {
    if(root.taskId().equals(taskId))
      return root;
    for(TaskNode task : leaves){
      if(task.taskId().equals(taskId))
        return task;
    }
    return null;
  }

  /**
   * @param taskId
   */
  public synchronized void setFailed(String taskId) {
    // TODO Auto-generated method stub
    
  }
}
