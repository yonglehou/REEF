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
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.nggroup.api.OperatorTopology;
import com.microsoft.reef.io.network.nggroup.api.OperatorTopologyStruct;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.tang.annotations.Name;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.SingleThreadStage;

/**
 *
 */
public class OperatorTopologyImpl implements OperatorTopology {

  private static final Logger LOG = Logger.getLogger(OperatorTopologyImpl.class.getName());


  private static final byte[] emptyByte = new byte[0];


  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String selfId;
  private final String driverId;
  private final Sender sender;
  private final Object topologyLock = new Object();

  private final BlockingQueue<GroupCommMessage> deltas = new LinkedBlockingQueue<>();
  private final EStage<GroupCommMessage> baseTopologyUpdateStage;
  private OperatorTopologyStruct baseTopology;
  private OperatorTopologyStruct effectiveTopology;
  private CountDownLatch topologyLockAquired = new CountDownLatch(1);

  @Inject
  public OperatorTopologyImpl(final Class<? extends Name<String>> groupName,
      final Class<? extends Name<String>> operName, final String selfId, final String driverId,
      final Sender sender) {
    super();
    this.groupName = groupName;
    this.operName = operName;
    this.selfId = selfId;
    this.driverId = driverId;
    this.sender = sender;
    baseTopologyUpdateStage = new SingleThreadStage<>(new EventHandler<GroupCommMessage>() {

      @Override
      public void onNext(final GroupCommMessage msg) {
        // TODO Auto-generated method stub
        assert(msg.getType()==Type.UpdateTopology);
        synchronized (topologyLock) {
          topologyLockAquired.countDown();
          updateBaseTopology();
        }
      }
    }, 5);
  }

  @Override
  public void handle(final GroupCommMessage msg) {
    final String srcId = msg.getSrcid();
    LOG.log(
        Level.FINE,
        "Message for " + msg.getDestid() + " operator "
            + msg.getOperatorname() + " in group-" + msg.getGroupname()
            + " from " + srcId);
    try {
      switch(msg.getType()) {
      case UpdateTopology:
        baseTopologyUpdateStage.onNext(msg);
        topologyLockAquired.await();
        //reset the Count Down Latch for the next update
        topologyLockAquired = new CountDownLatch(1);
        sendAckToDriver(msg);
        break;

      case ParentAdd:
      case ChildAdd:
        deltas.put(msg);
        break;

      case ParentDead:
      case ChildDead:
        deltas.put(msg);
        if(effectiveTopology!=null) {
          effectiveTopology.addAsData(msg);
          effectiveTopology.update(msg);
        }else {
          LOG.warning("Received a death message before effective topology was setup");
        }
        break;

        default:
          //Data msg
          if(effectiveTopology!=null) {
            effectiveTopology.addAsData(msg);
          }else {
            LOG.warning("Received a data message before effective topology was setup");
          }
      }
    } catch (final InterruptedException e) {
      throw new RuntimeException("InterruptedException while trying to put ctrl msg into delta queue", e);
    }
  }

  @Override
  public void sendToParent(final byte[] data, final Type msgType) {
    refreshEffectiveTopology();
    assert(effectiveTopology!=null);
    effectiveTopology.sendToParent(data,msgType);
  }

  @Override
  public void sendToChildren(final byte[] data, final Type msgType) {
    refreshEffectiveTopology();
    assert(effectiveTopology!=null);
    effectiveTopology.sendToChildren(data,msgType);
  }

  @Override
  public byte[] recvFromParent() {
    refreshEffectiveTopology();
    assert(effectiveTopology!=null);
    return effectiveTopology.recvFromParent();
  }

  @Override
  public List<byte[]> recvFromChildren() {
    refreshEffectiveTopology();
    assert(effectiveTopology!=null);
    return effectiveTopology.recvFromChildren();
  }

  private void refreshEffectiveTopology() {
    synchronized (topologyLock) {
      if(baseTopology==null) {
        createBaseTopology();
      }
      assert(baseTopology!=null);
      updateEffTopologyFromBaseTopology();
      assert(effectiveTopology!=null);
      final Set<GroupCommMessage> deletionDeltas = new HashSet<>();
      copyDeletionDeltas(deletionDeltas);
      effectiveTopology.update(deletionDeltas);
    }
  }

  /**
   *
   */
  private void createBaseTopology() {
    baseTopology = new OperatorTopologyStructImpl(groupName,operName,selfId,driverId,sender);
    updateBaseTopology();
  }

  private void updateBaseTopology() {
    try {
      baseTopology.setChanges(true);
      while(true) {
        final GroupCommMessage msg = deltas.take();
        if(msg.getType()==Type.TopologySetup) {
          if(!deltas.isEmpty()) {
            LOG.warning("The delta msg queue is not empty when I got " + msg.getType() + ". Something is fishy!!!!");
          }
          break;
        }
        baseTopology.update(msg);
        sendAckToDriver(msg);
      }
    } catch (final InterruptedException e) {
      throw new RuntimeException("InterruptedException while waiting for delta msg from driver", e);
    }
  }

  /**
   * @param msg
   */
  private void sendAckToDriver(final GroupCommMessage msg) {
    try {
      switch(msg.getType()) {
      case UpdateTopology:
        sender.send(Utils.bldGCM(groupName, operName, Type.TopologySetup, selfId, driverId, emptyByte));
        break;
      case ParentAdd:
        sender.send(Utils.bldGCM(groupName, operName, Type.ParentAdded, selfId, driverId, emptyByte));
        break;
      case ParentDead:
        sender.send(Utils.bldGCM(groupName, operName, Type.ParentRemoved, selfId, driverId, emptyByte));
        break;
      case ChildAdd:
        sender.send(Utils.bldGCM(groupName, operName, Type.ChildAdded, selfId, driverId, emptyByte));
        break;
      case ChildDead:
        sender.send(Utils.bldGCM(groupName, operName, Type.ChildRemoved, selfId, driverId, emptyByte));
        break;
      default:
        LOG.warning("Received a non control message for acknowledgement");
        throw new RuntimeException("Received a non control message for acknowledgement");
      }
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending ack to driver for delta msg " + msg.getType(), e);
    }
  }

  /**
   *
   */
  private void updateEffTopologyFromBaseTopology() {
    if(baseTopology.hasChanges()) {
      effectiveTopology = createFrom(baseTopology);
      baseTopology.setChanges(false);
    }
  }

  /**
   * @param topology
   * @return
   */
  private OperatorTopologyStruct createFrom(final OperatorTopologyStruct topology) {
    return new OperatorTopologyStructImpl(topology);
  }

  /**
   * @param deletionDeltas
   */
  private void copyDeletionDeltas(final Set<GroupCommMessage> deletionDeltas) {
    for(final GroupCommMessage msg : deltas) {
      final Type msgType = msg.getType();
      if(msgType==Type.ChildDead || msgType==Type.ParentDead) {
        deletionDeltas.add(msg);
      }
    }
  }

  @Override
  public String getSelfId() {
    return selfId;
  }

}
