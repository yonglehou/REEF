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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.io.network.nggroup.api.NeighborStatus;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;

/**
 *
 */
public class BroadcastHandlerImpl implements com.microsoft.reef.io.network.nggroup.api.BroadcastHandler{

  private static final Logger LOG = Logger.getLogger(BroadcastHandlerImpl.class.getName());

  private final ConcurrentMap<String, BlockingQueue<GroupCommMessage>> id2dataQue = new ConcurrentHashMap<>();

  private final BlockingQueue<GroupCommMessage> ctrlQue = new LinkedBlockingQueue<>();

  private final int numChildrenToWaitFor;

  private final CountDownLatch parentLatch;

  private final CountDownLatch childLatch;

  @Inject
  public BroadcastHandlerImpl(final int numParentsToWaitFor, final int numChildrenToWaitFor) {
    this.numChildrenToWaitFor = numChildrenToWaitFor;
    this.parentLatch = new CountDownLatch(numParentsToWaitFor);
    this.childLatch = new CountDownLatch(numChildrenToWaitFor);
  }

  @Override
  public synchronized void addNeighbor(final String id) {
    LOG.log(Level.INFO, "Adding {0} as one of the neighbors from which I can listen from", id);
    this.id2dataQue.put(id, new LinkedBlockingQueue<GroupCommMessage>());
  }

  @Override
  public synchronized void removeNeighbor(final String id) {
    LOG.log(Level.INFO, "Removing {0} as one of the neighbors from which I can listen from", id);
    this.id2dataQue.remove(id);
  }

  @Override
  public void waitForSetup(){
    try {
      LOG.info("Waiting for parent add");
      parentLatch.await();
      LOG.info("Parent added");
      LOG.info("Waiting for " + numChildrenToWaitFor + " children to be added");
      childLatch.await();
      LOG.info(numChildrenToWaitFor + " children added");
    } catch (final InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for setup of Broadcast operator", e);
    }
  }

  @Override
  public synchronized void onNext(final GroupCommMessage msg) {
    HandlerHelper.handleMsg(msg, this);
  }

  @Override
  public NeighborStatus updateTopology(){
    return HandlerHelper.updateTopology(this);
  }

  @Override
  public byte[] get(final String id) throws InterruptedException {
    LOG.log(Level.INFO, "\t\tget from {0}", id);

    if (!this.id2dataQue.containsKey(id)) {
      throw new RuntimeException("Can't receive from a non-child");
    }

    final GroupCommMessage gcm = id2dataQue.get(id).take();
    if (gcm.getType() == Type.ParentDead) {
      LOG.log(Level.WARNING, "\t\tGot parent dead msg from driver. Terminating wait and returning null");
      return null;
    }

    if(gcm.getMsgsCount()==1) {
      return gcm.getMsgs(0).toByteArray();
    } else {
      return null;
    }
  }

  @Override
  public ConcurrentMap<String, BlockingQueue<GroupCommMessage>> getDataQue() {
    return id2dataQue;
  }

  @Override
  public BlockingQueue<GroupCommMessage> getCtrlQue() {
    return ctrlQue;
  }

  @Override
  public CountDownLatch getParentLatch() {
    return parentLatch;
  }

  @Override
  public CountDownLatch getChildLatch() {
    return childLatch;
  }
}
