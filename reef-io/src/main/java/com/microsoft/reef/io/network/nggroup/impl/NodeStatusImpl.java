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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

import com.microsoft.reef.io.network.nggroup.api.TaskNode;
import com.microsoft.reef.io.network.nggroup.api.TaskNodeStatus;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.tang.annotations.Name;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.SingleThreadStage;

/**
 *
 */
public class NodeStatusImpl implements TaskNodeStatus {

  private static final Logger LOG = Logger.getLogger(NodeStatusImpl.class.getName());

  private final ConcurrentMap<Type, Set<String>> statusMap = new ConcurrentHashMap<>();
  private final EStage<GroupCommMessage> senderStage;
  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String taskId;
  private final String driverId;
  private final EStage<GroupCommMessage> msgSenderStage;
  private final EStage<GroupCommMessage> msgProcessorStage;
  private final Set<String> activeNeighbors = new HashSet<>();

  private final TaskNode node;



  public NodeStatusImpl(
      final EStage<GroupCommMessage> senderStage,
      final Class<? extends Name<String>> groupName,
      final Class<? extends Name<String>> operatorName,
      final String taskId,
      final String driverId,
      final TaskNode node) {
        this.senderStage = senderStage;
        this.groupName = groupName;
        this.operName = operatorName;
        this.taskId = taskId;
        this.driverId = driverId;
        this.node = node;

        msgSenderStage = new SingleThreadStage<>("NodeStatusMsgSenderStage", new EventHandler<GroupCommMessage>() {

          @Override
          public void onNext(final GroupCommMessage msg) {
            final String srcId = msg.getSrcid();
            final Type msgType = msg.getType();
            LOG.info(getQualifiedName() + "NodeStatusMsgSenderStage handling " + msgType + " msg from " + srcId);
            synchronized (statusMap) {
              LOG.info(getQualifiedName() + "NodeStatusMsgSenderStage acquired statusMap lock");
              Set<String> sources = new HashSet<>();
              final Set<String> oldSources = statusMap.putIfAbsent(msgType, sources);
              if(oldSources!=null) {
                sources = oldSources;
              }
              LOG.info(getQualifiedName() + "NodeStatusMsgSenderStage Adding " + srcId + " to sources");
              sources.add(srcId);
              LOG.info(getQualifiedName() + "NodeStatusMsgSenderStage sources for " + msgType + " are: " + sources);
            }
            LOG.info(getQualifiedName() + "NodeStatusMsgSenderStage Sending " + msgType + " msg from " + srcId + " to " + taskId);
            senderStage.onNext(msg);
          }
        }, 10);

        msgProcessorStage = new SingleThreadStage<>("NodeStatusMsgProcessorStage", new EventHandler<GroupCommMessage>() {

          @Override
          public void onNext(final GroupCommMessage gcm) {
            final String self = gcm.getSrcid();
            LOG.info(getQualifiedName() + "handling " + gcm.getType() + " msg from " + self + " for " + gcm.getDestid());
            synchronized (statusMap) {
              switch(gcm.getType()) {
              case ParentAdded:
                final String sourceParentAdded = gcm.getDestid();
                if(statusMap.containsKey(Type.ParentAdd)) {
                  final Set<String> sourceSet = statusMap.get(Type.ParentAdd);
                  if(sourceSet.contains(sourceParentAdded)) {
                    LOG.info(getQualifiedName() + "Removing " + sourceParentAdded + " from sources expecting ACK");
                    sourceSet.remove(sourceParentAdded);
                    activeNeighbors.add(sourceParentAdded);
                    node.chkAndSendTopSetup(sourceParentAdded);
                    if(sourceSet.isEmpty()) {
                      LOG.info(getQualifiedName() + "Empty source set. Removing");
                      statusMap.remove(Type.ParentAdd);
                      if(statusMap.isEmpty()) {
                        LOG.info(getQualifiedName() + "Empty status map.");
                        node.chkAndSendTopSetup();
                      }
                      else {
                        LOG.info(getQualifiedName() + "Status map non-empty" + statusMap);
                      }
                    }
                  } else {
                    LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage Got ParentAdded from a source(" + sourceParentAdded + ") to whom ParentAdd was not sent");
                  }
                }
                else {
                  LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage There were no ParentAdd msgs sent in the previous update cycle");
                }
                break;

              case ChildAdded:
                final String sourceChildAdded = gcm.getDestid();
                if(statusMap.containsKey(Type.ChildAdd)) {
                  final Set<String> sourceSet = statusMap.get(Type.ChildAdd);
                  if(sourceSet.contains(sourceChildAdded)) {
                    LOG.info(getQualifiedName() + "Removing " + sourceChildAdded + " from sources expecting ACK");
                    sourceSet.remove(sourceChildAdded);
                    activeNeighbors.add(sourceChildAdded);
                    node.chkAndSendTopSetup(sourceChildAdded);
                    if(sourceSet.isEmpty()) {
                      LOG.info(getQualifiedName() + "Empty source set. Removing");
                      statusMap.remove(Type.ChildAdd);
                      if(statusMap.isEmpty()) {
                        LOG.info(getQualifiedName() + "Empty status map.");
                        node.chkAndSendTopSetup();
                      }
                      else {
                        LOG.info(getQualifiedName() + "Status map non-empty" + statusMap);
                      }
                    }
                  } else {
                    LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage Got ChildAdded from a source(" + sourceChildAdded + ") to whom ChildAdd was not sent");
                  }
                }
                else {
                  LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage There were no ChildAdd msgs sent in the previous update cycle");
                }
                break;

              case ParentRemoved:
                final String sourceParentRemoved = gcm.getDestid();
                if(statusMap.containsKey(Type.ParentDead)) {
                  final Set<String> sourceSet = statusMap.get(Type.ParentDead);
                  if(sourceSet.contains(sourceParentRemoved)) {
                    LOG.info(getQualifiedName() + "Removing " + sourceParentRemoved + " from sources expecting ACK");
                    sourceSet.remove(sourceParentRemoved);
                    activeNeighbors.remove(sourceParentRemoved);
                    if(sourceSet.isEmpty()) {
                      node.chkAndSendTopSetup();
                    }
                  }else {
                    LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage Got ParentRemoved from a source(" + sourceParentRemoved + ") to whom ParentDead was not sent");
                  }
                }
                else {
                  LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage There were no ParentDead msgs sent in the previous update cycle");
                }
                break;

              case ChildRemoved:
                final String sourceChildRemoved = gcm.getDestid();
                if(statusMap.containsKey(Type.ChildDead)) {
                  final Set<String> sourceSet = statusMap.get(Type.ChildDead);
                  if(sourceSet.contains(sourceChildRemoved)) {
                    LOG.info(getQualifiedName() + "Removing " + sourceChildRemoved + " from sources expecting ACK");
                    sourceSet.remove(sourceChildRemoved);
                    activeNeighbors.remove(sourceChildRemoved);
                    if(sourceSet.isEmpty()) {
                      node.chkAndSendTopSetup();
                    }
                  }else {
                    LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage Got ChildRemoved from a source(" + sourceChildRemoved + ") to whom ChildDead was not sent");
                  }
                }
                else {
                  LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage There were no ChildDead msgs sent in the previous update cycle");
                }
                break;

                default:
                  LOG.warning(getQualifiedName() + "Non-ctrl msg " + gcm.getType() + " for " + gcm.getDestid() + " unexpected");
                  break;
              }
            }
          }
        }, 10);
  }

  @Override
  public boolean isActive(final String neighborId) {
    return activeNeighbors.contains(neighborId);
  }

  @Override
  public boolean amIActive() {
    return statusMap.isEmpty();
  }

  @Override
  public void sendMsg(final GroupCommMessage gcm) {
    msgSenderStage.onNext(gcm);
  }

  @Override
  public void setFailed() {
    synchronized (statusMap) {
      statusMap.clear();
      activeNeighbors.clear();
    }
  }

  @Override
  public void processMsg(final GroupCommMessage msg) {
    msgProcessorStage.onNext(msg);
  }

  /**
   * @return
   */
  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName) + ":" + taskId + " - ";
  }

}
