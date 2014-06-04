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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import com.microsoft.reef.io.network.nggroup.api.NodeStruct;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;

/**
 *
 */
public abstract class NodeStructImpl implements NodeStruct {

  private static final Logger LOG = Logger.getLogger(NodeStructImpl.class.getName());


  private final String id;
  private final BlockingQueue<GroupCommMessage> dataQue = new LinkedBlockingQueue<>();

  public NodeStructImpl(final String id) {
    super();
    this.id = id;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public void addData(final GroupCommMessage msg) {
    dataQue.add(msg);
  }

  @Override
  public byte[] getData() {
    GroupCommMessage gcm;
    try {
      gcm = dataQue.take();
    } catch (final InterruptedException e) {
      throw new RuntimeException("InterruptedException while waiting for data from " + id, e);
    }

    if(checkDead(gcm)) {
      return null;
    }

    if(gcm.getMsgsCount()==1) {
      return gcm.getMsgs(0).getData().toByteArray();
    } else {
      return null;
    }
  }

  public abstract boolean checkDead(final GroupCommMessage gcm);

}
