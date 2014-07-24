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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
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

class NodeTopology {
  HyperCubeNode node;
  Map<Integer, String> nodeTaskMap;
  Map<String, Integer> taskVersionMap;
  int baseIteration;
  int newIteration;
  // The reasons for updating
  boolean isFailed;
  boolean isNewNodeAdded;

  NodeTopology() {
    node = null;
    nodeTaskMap = null;
    taskVersionMap = null;
    baseIteration = -1;
    newIteration = -1;
    // The reasons for updating
    isFailed = false;
    isNewNodeAdded = false;
  }
}

public class HyperCubeTopoClient {

  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String selfID;

  private final BlockingQueue<GroupCommMessage> ctrlQueue;
  private final TreeMap<Integer, NodeTopology> nodeTopoMap;
  private final int version;

  public HyperCubeTopoClient(final Class<? extends Name<String>> groupName,
    final Class<? extends Name<String>> operName, final String selfId,
    final String driverId, final Sender sender, final int version) {
    this.groupName = groupName;
    this.operName = operName;
    this.selfID = selfId;
    this.version = version;

    ctrlQueue = new LinkedBlockingQueue<>();
    nodeTopoMap = new TreeMap<>();
  }

  public void handle(final GroupCommMessage msg) {
    // No topology change or topology updated.
    // Those two types of messages won't come here.
    if (msg.getVersion() != version) {
      printMsgInfo(msg, "Wrong version.");
      return;
    }
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
        "InterruptedException while trying to put ctrl msg into the queue", e);
    }
  }

  void waitForNewNodeTopology() {
    // Wait for the data
    GroupCommMessage msg = null;
    do {
      try {
        msg = ctrlQueue.take();
        printMsgInfo(msg, "Take from the ctrl queue.");
        // If this msg is mistakenly put into the queue, ignore it.
        if (msg.getVersion() != version) {
          msg = null;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    } while (msg == null);
    processNodeTopologyMsg(msg);
  }

  void processNodeTopologyMsg(GroupCommMessage msg) {
    // Decode the topology data
    NodeTopology nodeTopo = decodeNodeTopologyFromBytes(Utils.getData(msg));
    System.out.println(getQualifiedName() + nodeTopo.node.toString());
    // If there is an old topology agreed on the same iteration number
    // replace it.
    // The control flow in "apply" is sequential, if tasks didn't get topology
    // update for the current source add/dead message, it won't process the next
    // one. So there won't be two topologies coming at the same time.
    // It is also same to the driver, if the driver didn't get a message from
    // the task for the current source dead/add situation, it won't generate
    // next message to this task.
    nodeTopoMap.put(nodeTopo.newIteration, nodeTopo);
  }

  private NodeTopology decodeNodeTopologyFromBytes(byte[] bytes) {
    ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
    DataInputStream din = new DataInputStream(bin);
    NodeTopology nodeTopo = new NodeTopology();
    HyperCubeNode node = new HyperCubeNode("", -1);
    Map<Integer, String> nodeTaskMap = new HashMap<Integer, String>();
    Map<String, Integer> taskVersionMap = new HashMap<String, Integer>();
    nodeTopo.node = node;
    nodeTopo.nodeTaskMap = nodeTaskMap;
    nodeTopo.taskVersionMap = taskVersionMap;
    try {
      node.read(din);
      int mapSize = din.readInt();
      for (int i = 0; i < mapSize; i++) {
        int nodeID = din.readInt();
        String taskID = din.readUTF();
        int taskVersion = din.readInt();
        nodeTaskMap.put(nodeID, taskID);
        taskVersionMap.put(taskID, taskVersion);
      }
      nodeTopo.baseIteration = din.readInt();
      nodeTopo.newIteration = din.readInt();
      nodeTopo.isFailed = din.readBoolean();
      // We set the reason for topology update
      // if it is not for failure,
      // must be for the addition of new nodes.
      if (nodeTopo.isFailed) {
        nodeTopo.isNewNodeAdded = false;
      } else {
        nodeTopo.isNewNodeAdded = true;
      }
      din.close();
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
    return nodeTopo;
  }

  NodeTopology getNodeTopology(int iteration) {
    return nodeTopoMap.get(iteration);
  }

  List<Integer> removeOldNodeTopologies(int iteration) {
    List<Integer> rmKeys = new LinkedList<>();
    for (Entry<Integer, NodeTopology> entry : nodeTopoMap.entrySet()) {
      if (entry.getKey() < iteration) {
        rmKeys.add(entry.getKey());
      }
    }
    for (int key : rmKeys) {
      nodeTopoMap.remove(key);
    }
    return rmKeys;
  }

  NodeTopology getNewestNodeTopology() {
    Entry<Integer, NodeTopology> lastEntry = nodeTopoMap.lastEntry();
    if (lastEntry != null) {
      return lastEntry.getValue();
    }
    return null;
  }

  private void printMsgInfo(GroupCommMessage msg, String cmd) {
    System.out.println(getQualifiedName() + "Get " + msg.getType()
      + " msg from " + msg.getSrcid() + " with source version "
      + msg.getSrcVersion() + " with target version " + msg.getVersion() + ". "
      + cmd);
  }

  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName) + ":"
      + selfID + ":TopoClient:" + version + " - ";
  }
}
