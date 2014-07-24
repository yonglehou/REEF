/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.network.nggroup.impl;

import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.tang.annotations.Name;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 *
 */
public class CommGroupNetworkHandlerImpl implements
  com.microsoft.reef.io.network.nggroup.api.CommGroupNetworkHandler {

  private static final Logger LOG = Logger
    .getLogger(CommGroupNetworkHandlerImpl.class.getName());

  private final Map<Class<? extends Name<String>>, EventHandler<GroupCommMessage>> operHandlers =
    new ConcurrentHashMap<>();
  private final Map<Class<? extends Name<String>>, BlockingQueue<GroupCommMessage>> topologyNotifications =
    new ConcurrentHashMap<>();
    
  // Synchronize source dead and source add message processing on allreduce
  // topologies.
  private final Map<Class<? extends Name<String>>, GroupCommMessage> sourceAddMsgMap =
    new ConcurrentHashMap<>();
  private final Map<Class<? extends Name<String>>, GroupCommMessage> sourceDeadMsgMap =
    new ConcurrentHashMap<>();
  private final AtomicInteger totalNumAllReducers = new AtomicInteger(0);
  private final AtomicBoolean isAllReducerTopoChangeMsgBlocked =
    new AtomicBoolean(false);

  @Inject
  public CommGroupNetworkHandlerImpl() {
  }

  @Override
  public void register(final Class<? extends Name<String>> operName,
    final EventHandler<GroupCommMessage> operHandler) {
    operHandlers.put(operName, operHandler);
    // Count the total number of allreducers.
    if (operHandler.getClass().getName().equals(AllReducer.class.getName())) {
      totalNumAllReducers.incrementAndGet();
      System.out.println("Current total number of AllReducers: "
        + totalNumAllReducers.get());
    }
  }

  @Override
  public void addTopologyElement(final Class<? extends Name<String>> operName) {
    LOG.info("Creating LBQ for " + operName);
    topologyNotifications.put(operName,
      new LinkedBlockingQueue<GroupCommMessage>());
  }

  @Override
  public void onNext(final GroupCommMessage msg) {
    LOG.info("Get msg with type: " + msg.getType() + " with src id: "
      + msg.getSrcid() + " with source version: " + msg.getSrcVersion()
      + " with target id: " + msg.getDestid() + " with target version: "
      + msg.getVersion());
    System.out.println("Get msg with type: " + msg.getType() + " with src id: "
      + msg.getSrcid() + " with source version: " + msg.getSrcVersion()
      + " with target id: " + msg.getDestid() + " with target version: "
      + msg.getVersion());
    final Class<? extends Name<String>> operName =
      Utils.getClass(msg.getOperatorname());
    if (msg.getType() == Type.TopologyUpdated) {
      LOG.info("Got TopologyUpdate msg for " + operName
        + ". Adding to respective queue");
      topologyNotifications.get(operName).add(msg);
    } else if (msg.getType() == Type.TopologyChanges) {
      LOG.info("Got TopologyChanges msg for " + operName
        + ". Adding to respective queue");
      topologyNotifications.get(operName).add(msg);
    } else if (msg.getType() == Type.SourceAdd
      || msg.getType() == Type.SourceDead) {
      handleAllReduceTopoChangeMsg(msg);
    } else {
      operHandlers.get(operName).onNext(msg);
    }
  }

  @Override
  public byte[] waitForTopologyChanges(
    final Class<? extends Name<String>> operName) {
    try {
      LOG.info("Waiting for topology change msg for " + operName);
      return Utils.getData(topologyNotifications.get(operName).take());
    } catch (final InterruptedException e) {
      throw new RuntimeException(
        "InterruptedException while waiting for topology update of "
          + operName.getSimpleName(), e);
    }
  }

  @Override
  public GroupCommMessage waitForTopologyUpdate(
    final Class<? extends Name<String>> operName) {
    try {
      LOG.info("Waiting for topology update msg for " + operName);
      GroupCommMessage msg = topologyNotifications.get(operName).take();
      LOG.info("Get topology update msg for " + operName);
      return msg;
    } catch (final InterruptedException e) {
      throw new RuntimeException(
        "InterruptedException while waiting for topology update of "
          + operName.getSimpleName(), e);
    }
  }

  // New methods for allreduce topology synchronization

  @Override
  public synchronized void blockAllReduceTopoChangeMsg() {
    isAllReducerTopoChangeMsgBlocked.compareAndSet(false, true);
  }

  @Override
  public synchronized void unblockAllReduceTopoChangeMsg() {
    handleSourceAddMsgs();
    handleSourceDeadMsgs();
    isAllReducerTopoChangeMsgBlocked.compareAndSet(true, false);
  }

  private synchronized void handleAllReduceTopoChangeMsg(
    final GroupCommMessage msg) {
    // Accumulate the messages until all the allreducers get the same message.
    // If these messages are not processed, driver won't send new messages.
    final Class<? extends Name<String>> operName =
      Utils.getClass(msg.getOperatorname());
    if (msg.getType() == Type.SourceAdd) {
      sourceAddMsgMap.put(operName, msg);
      if (!isAllReducerTopoChangeMsgBlocked.get()) {
        handleSourceAddMsgs();
      }
    } else if (msg.getType() == Type.SourceDead) {
      sourceDeadMsgMap.put(operName, msg);
      if (!isAllReducerTopoChangeMsgBlocked.get()) {
        handleSourceDeadMsgs();
      }
    }
  }

  private void handleSourceAddMsgs() {
    if (sourceAddMsgMap.size() == totalNumAllReducers.get()) {
      System.out.println("Process SourceAdd on all the allreduce topologies.");
      for (Entry<Class<? extends Name<String>>, GroupCommMessage> entry : sourceAddMsgMap
        .entrySet()) {
        operHandlers.get(entry.getKey()).onNext(entry.getValue());
      }
      sourceAddMsgMap.clear();
    } else {
      System.out.println("SourceAdd map size: " + sourceAddMsgMap.size()
        + ", isAllReducerTopoChangeMsgBlocked? "
        + isAllReducerTopoChangeMsgBlocked);
    }
  }

  private void handleSourceDeadMsgs() {
    if (sourceDeadMsgMap.size() == totalNumAllReducers.get()) {
      System.out.println("Process SourceDead on all the allreduce topologies.");
      for (Entry<Class<? extends Name<String>>, GroupCommMessage> entry : sourceDeadMsgMap
        .entrySet()) {
        operHandlers.get(entry.getKey()).onNext(entry.getValue());
      }
      sourceDeadMsgMap.clear();
    } else {
      System.out.println("SourceDead map size: " + sourceDeadMsgMap.size()
        + ", isAllReducerTopoChangeMsgBlocked? "
        + isAllReducerTopoChangeMsgBlocked);
    }
  }
}
