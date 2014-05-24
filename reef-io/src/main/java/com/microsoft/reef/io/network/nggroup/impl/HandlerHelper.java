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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.reef.io.network.nggroup.api.NeighborStatus;
import com.microsoft.reef.io.network.nggroup.api.OperatorHandler;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;

/**
 *
 */
public class HandlerHelper {

  private static final Logger LOG = Logger.getLogger(HandlerHelper.class.getName());

  public static void handleMsg(
      final GroupCommMessage msg,
      final OperatorHandler handler) {
    final CountDownLatch parentLatch = handler.getParentLatch();
    final CountDownLatch childLatch = handler.getChildLatch();
    final BlockingQueue<GroupCommMessage> ctrlQue = handler.getCtrlQue();
    final ConcurrentMap<String, BlockingQueue<GroupCommMessage>> id2dataQue = handler.getDataQue();

    final String srcId = msg.getSrcid();
    LOG.log(
        Level.FINE,
        "Message for " + msg.getDestid() + " operator "
            + msg.getOperatorname() + " in group-" + msg.getGroupname()
            + " from " + srcId);
    try{
      switch (msg.getType()) {
      case ParentAdd:
        parentLatch .countDown();
        handler.addNeighbor(srcId);
        ctrlQue.put(msg);
        break;

      case ChildAdd:
        childLatch.countDown();
        handler.addNeighbor(srcId);
        ctrlQue.put(msg);
        break;

      case ParentDead:
      case ChildDead:
        handler.removeNeighbor(srcId);
        ctrlQue.put(msg);
        default:
        if (id2dataQue.containsKey(srcId)) {
          id2dataQue.get(srcId).put(msg);
        }
      }
    }catch(final InterruptedException e){
      throw new RuntimeException("Could not put " + msg + " into the queue", e);
    }
    LOG.info(msg.getType() + " handled");
  }

  public static NeighborStatus updateTopology(final OperatorHandler handler) {
    LOG.info("updating topology using ctrl msgs for handler " + handler.getClass().getName());
    final BlockingQueue<GroupCommMessage> ctrlQue = handler.getCtrlQue();
    final NeighborStatus neighborStatus = new NeighborStatusImpl();
    while(!ctrlQue.isEmpty()) {
      final GroupCommMessage ctrlMsg = ctrlQue.poll();
      final String from = ctrlMsg.getSrcid();
      neighborStatus.add(from,ctrlMsg.getType());
    }
    neighborStatus.updateDone();
    return neighborStatus;
  }
}
