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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.tang.annotations.Name;

/**
 * This class is modified from OperatorTopologyImpl, but we don't implement the
 * interface OperatorTopology now. In hyper cube topology, there is no parent
 * and children, but just a list of neighbors.
 * 
 * The methods in this class are not thread-safe. They should be carefully
 * synchronized by AllReducer.
 * 
 */
public class HyperCubeTopoClient {

  // private static final Logger LOG =
  // Logger.getLogger(HyperCubeTopoClient.class
  // .getName());
  // private static final byte[] EmptyByte = new byte[0];

  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String selfID;
  // private final String driverID;
  // private final Sender sender;

  private final BlockingQueue<GroupCommMessage> ctrlQueue =
    new LinkedBlockingQueue<>();

  private final NodeTopology nodeTopo;
  private int baseIteration;
  private int newIteration;
  private boolean isFailed;
  private boolean isNewNodeAdded;

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
    this.baseIteration = version;
    this.newIteration = version;
    this.nodeTopo = new NodeTopology();
    // this.driverID = driverId;
    // this.sender = sender;
  }

  public void handle(final GroupCommMessage msg) {
    // No topology change or topology updated.
    // Those two types of messages won't come here.
    System.out.println(getQualifiedName() + "Handling " + msg.getType()
      + " msg from " + msg.getSrcid());
    try {
      switch (msg.getType()) {
      case TopologySetup:
        ctrlQueue.put(msg);
        break;
      case UpdateTopology:
        ctrlQueue.put(msg);
      default:
      }
    } catch (final InterruptedException e) {
      throw new RuntimeException(
        "InterruptedException while trying to put ctrl msg into delta queue", e);
    }
  }

  void initialize() {
    // Just try to receive the topology
    getTopology();
  }

  void getTopology() {
    // Wait for the data
    GroupCommMessage msg = null;
    System.out.println(getQualifiedName()
      + "Wait for the topology from the driver.");
    try {
      msg = ctrlQueue.take();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    System.out.println(getQualifiedName() + "Got " + msg.getType()
      + " msg from " + msg.getSrcid());
    // Decode the topology data
    decodeNodeTopologyFromBytes(Utils.getData(msg));
    // LOG.info(nodeTopo.node.toString());
    System.out.println(nodeTopo.node.toString());
  }

  private NodeTopology decodeNodeTopologyFromBytes(byte[] bytes) {
    ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
    DataInputStream din = new DataInputStream(bin);
    HyperCubeNode node = new HyperCubeNode("", -1);
    ConcurrentMap<Integer, String> opTaskMap =
      new ConcurrentHashMap<Integer, String>();
    nodeTopo.node = node;
    nodeTopo.opTaskMap = opTaskMap;
    try {
      node.read(din);
      int opTaskMapSize = din.readInt();
      for (int i = 0; i < opTaskMapSize; i++) {
        opTaskMap.put(din.readInt(), din.readUTF());
      }
      baseIteration = din.readInt();
      newIteration = din.readInt();
      isFailed = din.readBoolean();
      // We set the reason for topology update
      // if it is not for failure,
      // must be for the addition of new nodes.
      if (isFailed) {
        isNewNodeAdded = false;
      } else {
        isNewNodeAdded = true;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
    return nodeTopo;
  }

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

  int getNewIteration() {
    return newIteration;
  }

  int getBaseIteration() {
    return baseIteration;
  }

  boolean isFailed() {
    return isFailed;
  }

  boolean isNewNodeAdded() {
    return isNewNodeAdded;
  }
}
