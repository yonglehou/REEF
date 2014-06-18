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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.tang.annotations.Name;

/**
 * This class is modified from OperatorTopologyImpl, but we don't imeplement the
 * interface OperatorTopology now. In hyper cube topology, there is no parent
 * and children, but just a list of neighbors.
 * 
 */
public class HyperCubeTopoClient {

  private static final Logger LOG = Logger.getLogger(HyperCubeTopoClient.class
    .getName());
  private static final byte[] EmptyByte = new byte[0];

  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String selfID;
  private final String driverID;
  private final Sender sender;
  private final Object topologyLock = new Object();

  private final BlockingQueue<GroupCommMessage> ctrlQueue =
    new LinkedBlockingQueue<>();
  // private CountDownLatch topologyLockAquired = new CountDownLatch(1);

  private NodeTopology nodeTopo;
  private int version;

  private class NodeTopology {
    private HyperCubeNode node;
    private ConcurrentMap<Integer, String> opTaskMap;
  }

  public HyperCubeTopoClient(final Class<? extends Name<String>> groupName,
    final Class<? extends Name<String>> operName, final String selfId,
    final String driverId, final Sender sender, final int version) {
    super();
    this.groupName = groupName;
    this.operName = operName;
    this.selfID = selfId;
    this.driverID = driverId;
    this.sender = sender;
    this.version = version;
  }

  public void handle(final GroupCommMessage msg) {
    final String srcId = msg.getSrcid();
    LOG.info(getQualifiedName() + "Handling " + msg.getType() + " msg from "
      + srcId);
    try {
      switch (msg.getType()) {
      case TopologySetup:
        LOG.info(getQualifiedName() + "Adding to deltas queue");
        ctrlQueue.put(msg);
        break;
      default:
      }
    } catch (final InterruptedException e) {
      throw new RuntimeException(
        "InterruptedException while trying to put ctrl msg into delta queue", e);
    }
  }

  public void initialize() {
    // refreshEffectiveTopology();
    // Just try to receive the topology
    getTopology();
    LOG.info(nodeTopo.node.toString());
    System.out.println(nodeTopo.node.toString());
  }

  private void getTopology() {
    // Wait for the data
    GroupCommMessage msg = null;
    LOG.info(getQualifiedName() + ": Get the topology from the driver.");
    while (true) {
      LOG.info(getQualifiedName() + ": Waiting for ctrl msgs");
      try {
        msg = ctrlQueue.take();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      LOG.info(getQualifiedName() + ": Got " + msg.getType() + " msg from "
        + msg.getSrcid());
      if (msg.getType() == Type.TopologySetup) {
        break;
      }
    }
    // Decode the data
    nodeTopo = decodeNodeTopologyFromBytes(Utils.getData(msg));
  }

  private NodeTopology decodeNodeTopologyFromBytes(byte[] bytes) {
    ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
    DataInputStream din = new DataInputStream(bin);
    HyperCubeNode node = new HyperCubeNode("", -1);
    ConcurrentMap<Integer, String> opTaskMap =
      new ConcurrentHashMap<Integer, String>();
    NodeTopology nodeTopo = new NodeTopology();
    nodeTopo.node = node;
    nodeTopo.opTaskMap = opTaskMap;
    try {
      node.read(din);
      int opTaskMapSize = din.readInt();
      for (int i = 0; i < opTaskMapSize; i++) {
        opTaskMap.put(din.readInt(), din.readUTF());
      }
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }

    return nodeTopo;
  }

  public String getSelfId() {
    return selfID;
  }

  /**
   * @return
   */
  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName) + ":"
      + selfID + " - ";
  }

  HyperCubeNode getNode() {
    return this.nodeTopo.node;
  }

  ConcurrentMap<Integer, String> getOpTaskMap() {
    return this.nodeTopo.opTaskMap;
  }
}
