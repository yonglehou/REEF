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

  private final ConcurrentCountingMap<Type, String> statusMap = new ConcurrentCountingMap<>();
  private final EStage<GroupCommMessage> senderStage;
  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String taskId;
  private final String driverId;
  private final EStage<GroupCommMessage> msgProcessorStage;
  private final Set<String> activeNeighbors = new HashSet<>();
  private final CountingMap<String> neighborStatus = new CountingMap<>();
  private final ResettingCDL topoSetup = new ResettingCDL(1);
  private final AtomicBoolean isTopoUpdateStageWaiting = new AtomicBoolean(false);
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
                //UpdateTopology might be queued up
                //but the task might have died. If so,
                //do not send this msg
                if(statusMap.isEmpty()) {
                  break;
                }
                //Send to taskId because srcId is usually always MasterTask
                LOG.info(getQualifiedName() + "NodeStatusMsgProcessorStage Sending UpdateTopology msg to " + taskId);
                senderStage.onNext(Utils.bldVersionedGCM(groupName, operatorName, node.getVersion(), Type.UpdateTopology, driverId, taskId, new byte[0]));
                break;
              case TopologySetup:
                synchronized (isTopoUpdateStageWaiting) {
                  if (!isTopoUpdateStageWaiting.compareAndSet(true, false)) {
                    LOG.info(getQualifiedName()
                        + "NodeStatusMsgProcessorStage Nobody waiting. Nothing to release");
                  }
                  else {
                    LOG.info(getQualifiedName()
                        + "NodeStatusMsgProcessorStage Releasing stage to send TopologyUpdated msg");
                    topoSetup.countDown();
                  }
                }
                break;
              case ParentAdded:
              case ChildAdded:
              case ParentRemoved:
              case ChildRemoved:
                if(!gcm.hasVersion()) {
                  throw new RuntimeException(getQualifiedName() + "NodeStatusMsgProcessorStage can only deal with versioned msgs");
                }
                final int rcvVersion = gcm.getVersion();
                final int nodeVersion = node.getVersion();
                if(rcvVersion<nodeVersion) {
                  LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage received a ver-" + rcvVersion
                      + " msg while expecting ver-" + nodeVersion + ". Discarding msg");
                  break;
                }
                if(nodeVersion<rcvVersion) {
                  LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage received a HIGHER ver-" + rcvVersion
                      + " msg while expecting ver-" + nodeVersion + ". Something fishy!!!");
                  break;
                }
                if(statusMap.containsKey(msgAcked)) {
                  if(statusMap.contains(msgAcked, sourceId)) {
                    LOG.info(getQualifiedName() + "NodeStatusMsgProcessorStage Removing " + sourceId +
                        " from sources expecting ACK");
                    statusMap.remove(msgAcked, sourceId);

                    if(isAddMsg(msgAcked)) {
                      neighborStatus.add(sourceId);
                    }
                    else if(isDeadMsg(msgAcked)) {
                      neighborStatus.remove(sourceId);
                    }
                    if(statusMap.notContains(sourceId)) {
                      if(neighborStatus.get(sourceId)>0) {
                        activeNeighbors.add(sourceId);
                        node.chkAndSendTopSetup(sourceId);
                      }
                      else {
                        LOG.warning(getQualifiedName() + sourceId + " is not a neighbor anymore");
                      }
                    }
                    else {
                      LOG.info(getQualifiedName() + "Not done processing " + sourceId + " acks yet. So it is still inactive");
                    }
                    chkAndSendTopoSetup(msgAcked);
                  } else {
                    LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage Got " + msgType +
                        " from a source(" + sourceId + ") to whom ChildAdd was not sent. " +
                        "Perhaps reset during failure. If context not indicative use ***CAUTION***");
                  }
                }
                else {
                  LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage There were no " + msgAcked +
                      " msgs sent in the previous update cycle. " +
                      "Perhaps reset during failure. If context not indicative use ***CAUTION***");
                }
                break;

                default:
                  LOG.warning(getQualifiedName() + "Non-ctrl msg " + gcm.getType() + " for " + gcm.getDestid() +
                      " unexpected");
                  break;
              }
            }
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

          private void chkAndSendTopoSetup(
              final Type msgDealt) {
            LOG.info(getQualifiedName() + "Checking if I am ready to send TopoSetup msg");
            if(statusMap.isEmpty()) {
              LOG.info(getQualifiedName() + "Empty status map.");
              node.chkAndSendTopSetup();
            }
            else {
              LOG.info(getQualifiedName() + "Status map non-empty" + statusMap);
            }
          }
        }, 10);
  }

  private boolean isDeadMsg(final Type msgAcked) {
    return msgAcked==Type.ParentDead || msgAcked==Type.ChildDead;
  }

  private boolean isAddMsg(final Type msgAcked) {
    return msgAcked==Type.ParentAdd || msgAcked==Type.ChildAdd;
  }

  @Override
  public void topoSetupSent() {
    neighborStatus.clear();
    if(insideUpdateTopology.compareAndSet(true, false)) {
      synchronized (statusMap) {
        statusMap.notifyAll();
      }
    } else {
      LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage Trying to set " +
          "UpdateTopology ended when it has already ended. Either initializing or " +
          "was reset during failure. If context not indicative use ***CAUTION***");
    }
  }

  @Override
  public boolean isActive(final String neighborId) {
    return activeNeighbors.contains(neighborId);
  }

  @Override
  public boolean amIActive() {
    return statusMap.isEmpty();
  }

  /**
   * This needs to happen in line rather than in
   * a stage because we need to note the messages
   * we send to the tasks before we start processing
   * msgs from the nodes.(Acks & Topology msgs)
   */
  @Override
  public void sendMsg(final GroupCommMessage gcm) {
    final String srcId = gcm.getSrcid();
    final Type msgType = gcm.getType();
    LOG.info(getQualifiedName() + "Handling " + msgType + " msg from " + srcId);
    synchronized (statusMap) {
      LOG.info(getQualifiedName() + "Acquired statusMap lock");
      while(insideUpdateTopology.get()) {
        LOG.info(getQualifiedName() + "Still processing UpdateTopology msg. " +
            "Need to block updates to statusMap till TopologySetup msg is sent");
        try {
          statusMap.wait();
        } catch (final InterruptedException e) {
          throw new RuntimeException("InterruptedException while waiting on statusMap", e);
        }
      }

      LOG.info(getQualifiedName() + "Adding " + srcId + " to sources");
      statusMap.add(msgType, srcId);
      LOG.info(getQualifiedName() + "Sources for " + msgType + " are: " + statusMap.get(msgType));
    }
    LOG.info(getQualifiedName() + "Sending " + msgType + " msg from " + srcId + " to " + taskId);
    senderStage.onNext(gcm);
  }

  @Override
  public void setFailed() {
    synchronized (statusMap) {
      statusMap.clear();
      activeNeighbors.clear();
      neighborStatus.clear();
      if(insideUpdateTopology.compareAndSet(true, false)) {
        statusMap.notifyAll();
      } else {
        LOG.warning(getQualifiedName() + "FaliureSet Not insideUpdateTopology. " +
        		"So not notifying");
      }
      synchronized (isTopoUpdateStageWaiting) {
        if (!isTopoUpdateStageWaiting.compareAndSet(true, false)) {
          LOG.info(getQualifiedName()
              + "NodeStatusMsgProcessorStage Nobody waiting. Nothing to release");
        }
        else {
          LOG.info(getQualifiedName()
              + "NodeStatusMsgProcessorStage Releasing stage to send TopologyUpdated msg");
          topoSetup.countDown();
        }
      }
    }
  }

  @Override
  public void setFailed(final String taskId) {
    synchronized (statusMap) {
     activeNeighbors.remove(taskId);
     neighborStatus.remove(taskId);
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
    synchronized (isTopoUpdateStageWaiting) {
      if (!isTopoUpdateStageWaiting.compareAndSet(false, true)) {
        final String msg = getQualifiedName()
            + "Don't expect someone else to be waiting. Something wrong!";
        LOG.severe(msg);
        throw new RuntimeException(msg);
      }
      topoSetup.awaitAndReset(1);
    }
  }

}
