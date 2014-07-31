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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.tang.annotations.Name;
import com.microsoft.wake.EventHandler;

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
    
  // Synchronize SourceDead and SourceAdd message processing on allreduce
  // topologies.
  private final Map<Class<? extends Name<String>>, GroupCommMessage> allreducerTopoMsgMap =
    new ConcurrentHashMap<>();
  private final AtomicInteger totalNumAllReducers = new AtomicInteger(0);
  private final AtomicBoolean isAllReducerTopoMsgBlocked =
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
      printLog("Current total number of AllReducers: "
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
    final Class<? extends Name<String>> operName =
      Utils.getClass(msg.getOperatorname());
    printLog(Utils.simpleName(operName) + " get message with type: "
      + msg.getType() + " with source id: " + msg.getSrcid()
      + " with source version: " + msg.getSrcVersion() + " with target id: "
      + msg.getDestid() + " with target version: " + msg.getVersion());
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
      handleAllReducerTopoMsg(msg);
    } else {
      EventHandler<GroupCommMessage> operHandler = operHandlers.get(operName);
      if (operHandler != null) {
        operHandler.onNext(msg);
      }
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
    isAllReducerTopoMsgBlocked.compareAndSet(false, true);
  }

  @Override
  public synchronized void unblockAllReduceTopoChangeMsg() {
    handleMsgs();
    isAllReducerTopoMsgBlocked.compareAndSet(true, false);
  }

  private synchronized void handleAllReducerTopoMsg(
    final GroupCommMessage msg) {
    // Accumulate the messages until all the allreducers get the same message.
    // If these messages are not processed, driver won't send new messages.

    List<Class<? extends Name<String>>> rmKeys = new LinkedList<>();
    boolean isOldMsg = false;
    for (Entry<Class<? extends Name<String>>, GroupCommMessage> entry : allreducerTopoMsgMap
      .entrySet()) {
      if (entry.getValue().getVersion() < msg.getVersion()) {
        rmKeys.add(entry.getKey());
      }
      if (entry.getValue().getVersion() > msg.getVersion()) {
        isOldMsg = true;
      }
      if (entry.getValue().getVersion() == msg.getVersion()
        && entry.getValue().getType() != msg.getType()) {
        rmKeys.add(entry.getKey());
      }
    }
    if (!isOldMsg) {
      // Remove
      for (Class<? extends Name<String>> rmKey : rmKeys) {
        GroupCommMessage rmMsg = allreducerTopoMsgMap.remove(rmKey);
        printLog("Remove msg with opername " + rmKey + " with type "
          + rmMsg.getType() + " with version " + rmMsg.getVersion());
      }
      final Class<? extends Name<String>> operName =
        Utils.getClass(msg.getOperatorname());
      allreducerTopoMsgMap.put(operName, msg);
      if (!isAllReducerTopoMsgBlocked.get()) {
        handleMsgs();
      }
    } else {
      printLog("Discard an old message.");
    }
  }

  private void handleMsgs() {
    if (allreducerTopoMsgMap.size() == totalNumAllReducers.get()) {
      printLog("Topo msg map size: " + allreducerTopoMsgMap.size()
        + ", process topo messages on all the allreduce topologies.");
      for (Entry<Class<? extends Name<String>>, GroupCommMessage> entry : allreducerTopoMsgMap
        .entrySet()) {
        try {
          operHandlers.get(entry.getKey()).onNext(entry.getValue());
        } catch (Exception e) {
        }
      }
      allreducerTopoMsgMap.clear();
      printLog("Topo msg map is clear.");
    } else {
      printLog("Topo msg map size: " + allreducerTopoMsgMap.size()
        + ", isAllReducerTopoChangeMsgBlocked? " + isAllReducerTopoMsgBlocked);
    }
  }

  private void printLog(String log) {
    System.out.println("CommGroupNetworkHandler - " + log);
    LOG.info("CommGroupNetworkHandler - " + log);
  }
}
