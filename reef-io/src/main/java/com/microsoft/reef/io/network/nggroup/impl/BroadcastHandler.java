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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.wake.Identifier;

/**
 * 
 */
public class BroadcastHandler implements com.microsoft.reef.io.network.nggroup.api.BroadcastHandler{
  
  private static final Logger LOG = Logger.getLogger(BroadcastHandler.class.getName());
  
  private final ConcurrentMap<String, BlockingQueue<GroupCommMessage>> id2dataQue = new ConcurrentHashMap<>();
  
  private final BlockingQueue<GroupCommMessage> ctrlQue = new LinkedBlockingQueue<>();

  @Inject
  public BroadcastHandler() {  }
  
  public synchronized void addNeighbor(final String id) {
    LOG.log(Level.FINEST, "Adding {0} as one of the neighbors from which I can listen from", id);
    this.id2dataQue.put(id, new LinkedBlockingQueue<GroupCommMessage>());
  }

  public synchronized void removeNeighbor(final Identifier id) {
    LOG.log(Level.FINEST, "Removing {0} as one of the neighbors from which I can listen from", id);
    this.id2dataQue.remove(id);
  }

  @Override
  public void onNext(GroupCommMessage msg) {
    final String srcId = msg.getSrcid();
    LOG.log(
        Level.FINE,
        "Message for " + msg.getDestid() + "  operator "
            + msg.getOperatorname() + " in group-" + msg.getGroupname()
            + " from " + srcId);
    try{
      switch(msg.getType()){
      case ParentAdd:
      case ChildAdd:
        this.ctrlQue.put(msg);
        break;
      case SourceDead:
        this.ctrlQue.put(msg);
        default:
          if(this.id2dataQue.containsKey(srcId)){
            this.id2dataQue.get(srcId).put(msg);
          }
      }
    }catch(InterruptedException e){
      throw new RuntimeException("Could not put " + msg + " into the queue", e);
    }
  }

  @Override
  public byte[] get(String id) throws InterruptedException {
    LOG.log(Level.FINEST, "\t\tget from {0}", id);
    
    if (!this.id2dataQue.containsKey(id)) {
      throw new RuntimeException("Can't receive from a non-child");
    }

    final GroupCommMessage gcm = id2dataQue.get(id).take();
    if (gcm.getType() == Type.SourceDead) {
      LOG.log(Level.WARNING, "\t\tGot src dead msg from driver. Terminating wait and returning null");
      return null;
    }
    
    if(gcm.getMsgsCount()==1)
      return gcm.getMsgs(0).toByteArray();
    else
      return null;
  }

}
