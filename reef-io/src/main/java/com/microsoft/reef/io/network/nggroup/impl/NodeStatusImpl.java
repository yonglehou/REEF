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
import java.util.concurrent.atomic.AtomicBoolean;
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
  private final ResettingCDL topoSetup = new ResettingCDL(1);
  private final AtomicBoolean insideUpdateTopology = new AtomicBoolean(false);

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
              while(insideUpdateTopology.get()) {
                LOG.info(getQualifiedName() + "NodeStatusMsgSenderStage Still processing UpdateTopology msg. " +
                		"Need to block updates to statusMap till TopologySetup msg is sent");
                try {
                  statusMap.wait();
                } catch (final InterruptedException e) {
                  throw new RuntimeException("InterruptedException while waiting on statusMap", e);
                }
              }

              statusMap.putIfAbsent(msgType, new HashSet<String>());
              final Set<String> sources = statusMap.get(msgType);

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
            LOG.info(getQualifiedName() + "NodeStatusMsgProcessorStage handling " + gcm.getType() + " msg from " + self + " for " + gcm.getDestid());
            final Type msgType = gcm.getType();
            final Type msgAcked = getAckedMsg(msgType);
            final String sourceId = gcm.getDestid();
            synchronized (statusMap) {
              switch(msgType) {
              case UpdateTopology:
                if(!insideUpdateTopology.compareAndSet(false, true)) {
                  LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage Got an UpdateTopology when I was still updating topology");
                }
                //Send to taskId because srcId is usually always MasterTask
                LOG.info(getQualifiedName() + "Sending UpdateTopology msg to " + taskId);
                senderStage.onNext(Utils.bldGCM(groupName, operatorName, Type.UpdateTopology, driverId, taskId, new byte[0]));
                break;
              case TopologySetup:
                LOG.info(getQualifiedName() + "Releasing stage to send TopologyUpdated msg");
                topoSetup.countDown();
                break;
              case ParentAdded:
              case ChildAdded:
              case ParentRemoved:
              case ChildRemoved:
                if(statusMap.containsKey(msgAcked)) {
                  final Set<String> sourceSet = statusMap.get(msgAcked);
                  if(sourceSet.contains(sourceId)) {
                    LOG.info(getQualifiedName() + "NodeStatusMsgProcessorStage Removing " + sourceId +
                        " from sources expecting ACK");
                    sourceSet.remove(sourceId);
                    if(isAddMsg(msgAcked)) {
                      activeNeighbors.add(sourceId);
                      node.chkAndSendTopSetup(sourceId);
                    }
                    else if(isDeadMsg(msgAcked)) {
                      activeNeighbors.remove(sourceId);
                    }
                    final boolean topoSetupSent = chkAndSendTopoSetup(sourceSet, msgAcked);
                    if(topoSetupSent) {
                      if(!insideUpdateTopology.compareAndSet(true, false)) {
                        LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage Trying to set " +
                        		"UpdateTopology ended when it has already ended");
                      }
                      statusMap.notifyAll();
                    }
                  } else {
                    LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage Got " + msgType +
                        " from a source(" + sourceId + ") to whom ChildAdd was not sent");
                  }
                }
                else {
                  LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage There were no " + msgAcked +
                      " msgs sent in the previous update cycle");
                }
                break;

                default:
                  LOG.warning(getQualifiedName() + "Non-ctrl msg " + gcm.getType() + " for " + gcm.getDestid() +
                      " unexpected");
                  break;
              }
            }
          }

          private boolean isDeadMsg(final Type msgAcked) {
            return msgAcked==Type.ParentDead || msgAcked==Type.ChildDead;
          }

          private boolean isAddMsg(final Type msgAcked) {
            return msgAcked==Type.ParentAdd || msgAcked==Type.ChildAdd;
          }

          private Type getAckedMsg(final Type msgType) {
            switch(msgType) {
            case ParentAdded:
              return Type.ParentAdd;
            case ChildAdded:
              return Type.ChildAdd;
            case ParentRemoved:
              return Type.ParentDead;
            case ChildRemoved:
              return Type.ChildDead;
              default:
                return msgType;
            }
          }

          private boolean chkAndSendTopoSetup(
              final Set<String> sourceSet,
              final Type msgDealt) {
            LOG.info(getQualifiedName() + "Checking if I am ready to send TopoSetup msg");
            if(sourceSet.isEmpty()) {
              LOG.info(getQualifiedName() + "Empty source set. Removing");
              statusMap.remove(msgDealt);
              if(statusMap.isEmpty()) {
                LOG.info(getQualifiedName() + "Empty status map.");
                return node.chkAndSendTopSetup();
              }
              else {
                LOG.info(getQualifiedName() + "Status map non-empty" + statusMap);
              }
            } else {
              LOG.info(getQualifiedName() + "Source set not empty" + statusMap);
            }
            return false;
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

  @Override
  public boolean hasChanges() {
    return !statusMap.isEmpty();
  }

  @Override
  public void waitForTopologySetup() {
    topoSetup.awaitAndReset(1);
  }

}
