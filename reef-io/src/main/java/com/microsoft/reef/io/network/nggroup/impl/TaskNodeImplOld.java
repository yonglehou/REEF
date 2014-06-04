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
import java.util.HashSet;
import java.util.List;
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
public class TaskNodeImplOld implements TaskNode {

  private static final Logger LOG = Logger.getLogger(TaskNodeImpl.class.getName());

  private final String taskId;
  private TaskNode parent;
  private final List<TaskNode> children = new ArrayList<>();
  private final Set<String> activeNeighbors = new HashSet<>();
  private final Object ackLock = new Object();

  private boolean running = false;
  private String driverId;
  private boolean topoSetup = false;
  private final EStage<GroupCommMessage> senderStage;
  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;

  public TaskNodeImplOld(
      final EStage<GroupCommMessage> senderStage,
      final Class<? extends Name<String>> groupName,
      final Class<? extends Name<String>> operName,
      final String taskId,
      final String driverId,
      final TaskNode parent,
      final boolean running) {
    super();
    this.senderStage = senderStage;
    this.groupName = groupName;
    this.operName = operName;
    this.taskId = taskId;
    this.parent = parent;
    this.running = running;
  }

  public TaskNodeImplOld(
      final EStage<GroupCommMessage> senderStage,
      final Class<? extends Name<String>> groupName,
      final Class<? extends Name<String>> operName,
      final String taskId,
      final String driverId,
      final TaskNode parent) {
    super();
    this.senderStage = senderStage;
    this.groupName = groupName;
    this.operName = operName;
    this.taskId = taskId;
    this.parent = parent;
  }

  /**
   * @param taskId
   */
  public TaskNodeImplOld(
      final EStage<GroupCommMessage> senderStage,
      final Class<? extends Name<String>> groupName,
      final Class<? extends Name<String>> operName,
      final String taskId,
      final String driverId) {
    super();
    this.senderStage = senderStage;
    this.groupName = groupName;
    this.operName = operName;
    this.taskId = taskId;
    this.driverId = driverId;
  }

  @Override
  public void addChild(final TaskNode child) {
    children.add(child);
  }

  @Override
  public void addActiveNeighbor(final String neighborId) {
    LOG.info(getQualifiedName() + "Adding " + neighborId + " as an active neighbor of.");
    activeNeighbors.add(neighborId);
  }

  @Override
  public void removeActiveNeighbor(final String neighborId) {
    LOG.info(getQualifiedName() + "Removing " + neighborId + " as an active neighbor of.");
    activeNeighbors.remove(neighborId);
  }

  @Override
  public boolean isNeighborActive(final String neighborId) {
    return activeNeighbors.contains(neighborId);
  }

  @Override
  public void setParent(final TaskNode parent) {
    this.parent = parent;
  }

  @Override
  public TaskNode getParent() {
    return parent;
  }

  @Override
  public String taskId() {
    return taskId;
  }

  @Override
  public void setRunning(final boolean b) {
    running = b;
    activeNeighbors.clear();
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public void handle(final GroupCommMessage gcm) {
    final String self = gcm.getSrcid();
    LOG.info(getQualifiedName() + "handling " + gcm.getType() + " msg from " + self + " for " + gcm.getDestid());
    synchronized(ackLock) {
      switch(gcm.getType()) {
      case UpdateTopology:
        assert(parent==null);
        LOG.info(getQualifiedName() + "Sending UpdateTopology msg to root " + taskId);
        senderStage.onNext(Utils.bldGCM(groupName, operName, Type.UpdateTopology, driverId, taskId, new byte[0]));
        break;
      case TopologySetup:
        assert(parent==null);
        LOG.info(getQualifiedName() + "Sending TopologyUpdated msg to root " + taskId);
        senderStage.onNext(Utils.bldGCM(groupName, operName, Type.TopologyUpdated, driverId, taskId, new byte[0]));
        break;
      case ParentAdded:
        final String parentId = gcm.getDestid();
        addActiveNeighbor(parentId);
        chkAndSendTopSetup();
        parent.chkAndSendTopSetup();
        break;
      case ChildAdded:
        final String childId = gcm.getDestid();
        addActiveNeighbor(childId);
        chkAndSendTopSetup();
        final TaskNode childNode = findTask(childId);
        assert(childNode!=null);
        childNode.chkAndSendTopSetup();
        break;
      case ParentRemoved:
      case ChildRemoved:
        final String destId = gcm.getDestid();
        removeActiveNeighbor(destId);
        chkAndSendTopSetup();
        break;
        default:
          LOG.warning("Non-ctrl msg " + gcm.getType() + " from " + gcm.getSrcid() + " unexpected");
          break;
      }
    }
  }

  /**
   * @param childId
   * @return
   */
  private TaskNode findTask(final String childId) {
    for (final TaskNode child : children) {
      if(child.taskId().equals(childId)) {
        return child;
      }
    }
    return null;
  }

  /**
   * @return
   */
  private boolean activeNeighborOfAllChildren() {
    for (final TaskNode child : children) {
      if(!child.isNeighborActive(taskId)) {
        LOG.info(getQualifiedName() + "Not an active neighbor of child " + child.taskId());
        return false;
      }
    }
    LOG.info(getQualifiedName() + "Active neighbor of all children");
    return true;
  }

  /**
   * @return
   */
  private boolean allChildrenActive() {
    for (final TaskNode child : children) {
      final String childId = child.taskId();
      if(!isNeighborActive(childId)) {
        LOG.info(getQualifiedName() + childId + " not active yet");
        return false;
      }
    }
    LOG.info(getQualifiedName() + "All children active");
    return true;
  }

  @Override
  public void chkAndSendTopSetup() {
    synchronized (ackLock) {
      LOG.info(getQualifiedName()
          + "Checking if I am ready to send TopoSetup msg");
      if (topoSetup) {
        LOG.info(getQualifiedName() + "topology setup msg sent already");
        return;
      }
      final boolean parentActive = parentActive();
      final boolean allChildrenActive = allChildrenActive();
      if (parentActive && allChildrenActive) {
        final boolean activeNeighborOfParent = activeNeighborOfParent();
        final boolean activeNeighborOfAllChildren = activeNeighborOfAllChildren();
        if (activeNeighborOfParent && activeNeighborOfAllChildren) {
          LOG.info(getQualifiedName() + " Sending TopoSetup msg to " + taskId);
          senderStage.onNext(Utils.bldGCM(groupName, operName,
              Type.TopologySetup, driverId, taskId, new byte[0]));
          topoSetup = true;
        } else {
          if (!activeNeighborOfParent) {
            LOG.info(getQualifiedName()
                + "I am not an active neighbor of parent "
                + (parent != null ? parent.taskId() : "NULL"));
          }
          if (!activeNeighborOfAllChildren) {
            LOG.info(getQualifiedName()
                + "I am not an active neighbor of all children");
          }
        }
      } else {
        if (!parentActive) {
          LOG.info(getQualifiedName() + "parent "
              + (parent != null ? parent.taskId() : "NULL") + " not active yet");
        }
        if (!allChildrenActive) {
          LOG.info(getQualifiedName() + "not all children active yet");
        }
      }
    }
  }

  /**
   * @return
   */
  private boolean activeNeighborOfParent() {
    if(parent==null) {
      LOG.info(getQualifiedName() + "Parent null. Perhaps I am root. So I am an active neghbor of non-existent parent");
      return true;
    }
    if(parent.isNeighborActive(taskId)) {
      LOG.info(getQualifiedName() + "I am an active neighbor of parent " + parent.taskId());
      return true;
    }
    return false;
  }

  /**
   * @return
   */
  private boolean parentActive() {
    if(parent==null) {
      LOG.info(getQualifiedName() + "Parent null. Perhaps I am root. So I have a non-existent active neghbor");
      return true;
    }
    if(isNeighborActive(parent.taskId())) {
      LOG.info(getQualifiedName() + parent.taskId() + " is an active neghbor");
      return true;
    }
    return false;
  }

  /**
   * @return
   */
  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName) + ":" + taskId + " - ";
  }

  @Override
  public void setFailed() {
    // TODO Auto-generated method stub

  }

  @Override
  public void setRunning() {
    // TODO Auto-generated method stub

  }

  @Override
  public void processMsg(final GroupCommMessage msg) {
    // TODO Auto-generated method stub

  }

  @Override
  public void processParentRunning() {
    // TODO Auto-generated method stub

  }

  @Override
  public void processChildRunning(final String childId) {
    // TODO Auto-generated method stub

  }

  @Override
  public void processChildDead(final String childId) {
    // TODO Auto-generated method stub

  }

  @Override
  public void processParentDead() {
    // TODO Auto-generated method stub

  }

  @Override
  public void chkAndSendTopSetup(final String source) {
    // TODO Auto-generated method stub

  }
}
