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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.reef.io.network.group.operators.GroupCommOperator;
import com.microsoft.reef.io.network.nggroup.api.OperatorSpec;
import com.microsoft.reef.io.network.nggroup.api.Topology;
import com.microsoft.reef.io.network.nggroup.impl.config.AllReduceOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.DataCodec;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.ReduceFunctionParam;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.TaskVersion;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.wake.EStage;

/**
 * At current stage, this topology is designed for "allreduce" operation with
 * bidirectional exchange algorithm (HyperCube Topology). This class is modified
 * from FlatTopology
 * 
 * @author zhangbj
 */
public class HyperCubeTopology implements Topology {

  private static final Logger LOG = Logger.getLogger(HyperCubeTopology.class
    .getName());
  private static final byte[] EmptyByteArr = new byte[0];

  /**
   * A thread pool initialized in GroupCommDriverImpl. It is used to send
   * messages to tasks (with 5 threads)
   */
  private final EStage<GroupCommMessage> senderStage;
  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private OperatorSpec operatorSpec;
  private final String driverID;
  private final AtomicBoolean isInitialized = new AtomicBoolean(false);

  // New data structures for hype cube topology
  /**
   * This is used to generate hypercube ID (starts from 0, initialized as -1)
   * for each task.This is also used to track the max ID in the hyper cube.
   */
  private int hyperCubeNodeMaxID = -1;
  /**
   * The dimension level or the total number dimensions of the current hyper
   * cube (starts from 1, initialized as 0). This is calculated base on the max
   * ID of the hyper cube. See if we need to make this variable atomic.
   */
  private int dimensionLevel = 0;
  /** This is the mapping between task ID and hypercube node ID */
  private final Map<String, Integer> taskMap = new TreeMap<>();
  /** This is the mapping between hype cube ID and hyper cube node */
  private final Map<Integer, HyperCubeNode> nodeMap = new TreeMap<>();
  /** List of node ids which from failed nodes and make them be reusable */
  private final Queue<Integer> freeNodeIDs = new LinkedBlockingQueue<Integer>();
  /** The total number of tasks initialized */
  private int numTasks = 0;
  private AtomicInteger numRunningTasks = new AtomicInteger(0);

  /** Iteration is used to track the iteration number of allreduce operation */
  private final AtomicInteger iteration = new AtomicInteger(0);
  /** This is used to collect all fail task ids */
  private final Set<String> failedTasks = new HashSet<>();
  private final Set<String> failedTaskNeighbors = new HashSet<>();
  /** This is used to collect all acks about failures */
  private final Map<String, Integer> failureReports = new HashMap<>();

  public HyperCubeTopology(final EStage<GroupCommMessage> senderStage,
    final Class<? extends Name<String>> groupName,
    final Class<? extends Name<String>> operatorName, final String driverID,
    final int numberOfTasks) {
    this.senderStage = senderStage;
    this.groupName = groupName;
    this.operName = operatorName;
    this.driverID = driverID;
    this.numTasks = numberOfTasks;
  }

  @Override
  public void setRoot(final String rootId) {
    // No root id. We could assign a root id in future if we use this topology
    // to do broadcast or reduce.
  }

  @Override
  public void setOperSpec(final OperatorSpec spec) {
    this.operatorSpec = spec;
  }

  @Override
  public Configuration getConfig(final String taskID) {
    // The topology configuration binds with the sender receiver implementation!
    // Sync with the current iteration number
    final int version = iteration.get();
    final JavaConfigurationBuilder jcb =
      Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(DataCodec.class, operatorSpec.getDataCodecClass());
    jcb.bindNamedParameter(TaskVersion.class, Integer.toString(version));
    final AllReduceOperatorSpec allreduceOperatorSpec =
      (AllReduceOperatorSpec) operatorSpec;
    jcb.bindNamedParameter(ReduceFunctionParam.class,
      allreduceOperatorSpec.getRedFuncClass());
    if (operatorSpec instanceof AllReduceOperatorSpec) {
      jcb.bindImplementation(GroupCommOperator.class, AllReducer.class);
    }
    return jcb.build();
  }

  @Override
  public synchronized void addTask(final String taskID) {
    // Invoked by CommunicationGroupDriverImpl addTask
    // Tasks are always added before launching and setRunning!
    if (!isInitialized.get()) {
      int nodeID = -1;
      if (!freeNodeIDs.isEmpty()) {
        nodeID = freeNodeIDs.poll();
      } else {
        // Increase the current max ID to get next ID and new max ID
        // New nodes are always added with IDs next larger than the current
        // max
        hyperCubeNodeMaxID++;
        // Calculate the current dimension level based on the new max ID
        this.dimensionLevel = updateDimensionLevel(hyperCubeNodeMaxID);
        nodeID = hyperCubeNodeMaxID;
      }
      System.out.println(getQualifiedName() + "Adding hyper cube node "
        + taskID + " with node id " + nodeID + " at the dimension level: "
        + this.dimensionLevel);
      addHyperCubeNode(taskID, nodeID, hyperCubeNodeMaxID, dimensionLevel,
        taskMap, nodeMap);
    }
  }

  void addTask(final String taskID, final int nodeID) {
    if (!nodeMap.containsKey(nodeID)) {
      addHyperCubeNode(taskID, nodeID, hyperCubeNodeMaxID, dimensionLevel,
        taskMap, nodeMap);
    }
  }

  /**
   * When a new task is added, invoke this method to add a task to a node in the
   * hypercube. This method is required to be synchronized because it may be
   * invoked by multiple threads.
   * 
   */
  private Set<Integer> addHyperCubeNode(final String taskID, final int nodeID,
    final int maxNodeID, final int dimLevel, Map<String, Integer> taskMap,
    Map<Integer, HyperCubeNode> nodeMap) {
    HyperCubeNode node = new HyperCubeNode(taskID, nodeID);
    Set<Integer> modifiedNeighbors =
      createNeighborList(node, maxNodeID, dimLevel, nodeMap);
    taskMap.put(taskID, nodeID);
    nodeMap.put(nodeID, node);
    return modifiedNeighbors;
  }

  /**
   * Here we create a node's neighbor op list based on the knowledge of the
   * current topology (a topology which can work). We need to lock the current
   * topology (taskMap and nodeMap) and add this node to the current topology.
   * We return modified neighbors. Generally the cost of adding a new node is
   * logP. But if the original topology is incomplete (which means some nodes
   * are dead and the topology is not perfect hypercube but is adjusted for the
   * failure), the cost may getting higher.
   * 
   */
  private Set<Integer> createNeighborList(HyperCubeNode node, int maxNodeID,
    int dimLevel, Map<Integer, HyperCubeNode> nodeMap) {
    Set<Integer> modifiedNeighbors = new HashSet<Integer>();
    List<List<int[]>> neighborOpList = node.getNeighborOpList();
    int nodeID = node.getNodeID();
    // Initialization
    int moduloBase = 2;
    int moduloBaseHalf = moduloBase / 2;
    int moduloResult = -1;
    int neighborID = -1;
    HyperCubeNode neighborNode = null;
    List<int[]> neighborOpDimList = null;
    int[] neighborOp = null;
    // We create op entry of this node with
    // {neighborID, opID} on each dimension.
    // For neighbor ID, its range is [0, maxID].
    // For op ID, 0 is receive, 1 is send, 2 is send/receive
    // The first entry is always reserved for pairing or sending,
    // because this node always need to send the data out.
    // There are 5 cases:
    // 1. send/receive to/from a neighbor (pairing)
    // 2. send/receive to/from a neighbor (pairing)
    // + receive from other neighbors
    // 3. send to a neighbor
    // 4. send to a neighbor + receive from several other neighbors
    // 5. No sending or pairing, {-2, -1} or {-2, -1}
    // {-1, -1} means no neighbor for sending or pairing
    // {-2, -1} means no sending or pairing because of the dead zone.
    for (int i = 0; i < dimLevel; i++) {
      neighborOpDimList = new ArrayList<int[]>();
      // Get the default op
      neighborOp = new int[] { -1, -1 };
      // Generate the pairing neighbor
      moduloResult = nodeID % moduloBase;
      // Get your neighbor ID from left or right
      if (moduloResult >= moduloBaseHalf) {
        neighborID = nodeID - moduloBaseHalf;
      } else {
        neighborID = nodeID + moduloBaseHalf;
      }
      neighborNode = nodeMap.get(neighborID);
      if (neighborNode != null) {
        // Pairing node is found!
        neighborOp[0] = neighborID;
        neighborOp[1] = 2;
        // System.out.println("Neighbor node: " + neighborID);
      } else {
        // Find another neighbor, the pairing node may be dead or doesn't exist
        neighborID =
          findAltNeighborInZone(nodeID, neighborID, maxNodeID, moduloBase, i,
            nodeMap);
        if (neighborID >= 0) {
          // If we get an alternative ID...
          // Sending node is found
          neighborOp[0] = neighborID;
          neighborOp[1] = 1;
        } else {
          // Dead zone or non-existing neighbor
          neighborOp[0] = neighborID;
          neighborOp[1] = -1;
        }
      }
      // System.out.println("Neighbor node: " + neighborOp[0] +
      // ", neighbor op: + neighborOp[1]);
      // Add this op
      neighborOpDimList.add(neighborOp);
      // If we get this neighbor...
      if (neighborID >= 0) {
        modifiedNeighbors.add(neighborID);
        // Get the op list on the neighbor at dimension i
        List<List<int[]>> nOpList = nodeMap.get(neighborID).getNeighborOpList();
        List<int[]> nOpDimList = null;
        // If this neighbor currently doesn't have dimension i,
        // add default op {-1, -1} on each dimension <= i.
        if ((nOpList.size() - 1) < i) {
          for (int j = nOpList.size(); j <= i; j++) {
            nOpDimList = new ArrayList<int[]>();
            nOpDimList.add(new int[] { -1, -1 });
            nOpList.add(nOpDimList);
          }
        }
        nOpDimList = nOpList.get(i);
        // If the neighbor says this node is from a dead zone
        // Set pairing with this neighbor (even original op is "sending")
        // Receiving data from all other nodes in the zone where neighbor is.
        if (nOpDimList.get(0)[0] == -2 && nOpDimList.get(0)[1] == -1) {
          // Get all other nodes in the neighbor zone
          List<Integer> nodeIDsInZone =
            findAllOtherNodesInZone(neighborID, moduloBase, maxNodeID, nodeMap);
          int[] op = null;
          for (int id : nodeIDsInZone) {
            op = nodeMap.get(id).getNeighborOpList().get(i).get(0);
            // Add sending
            op[0] = nodeID;
            op[1] = 1;
            // Add receiving
            neighborOpDimList.add(new int[] { id, 0 });
            // All other nodes in this neighbor's zone is modified!
            modifiedNeighbors.add(id);
          }
          // Set the neighbor as a pair.
          neighborOp[0] = neighborID;
          neighborOp[1] = 2;
          nOpDimList.get(0)[0] = nodeID;
          nOpDimList.get(0)[1] = 2;
        } else {
          // If no dead zone...
          if (neighborOp[1] == 1) {
            if (nOpDimList.get(0)[0] == -1 && nOpDimList.get(0)[1] == -1) {
              // If no pair or sending in original topology
              // No operation.
              // This should not happen
              // because this neighbor doesn't need new data
            } else if (nOpDimList.get(0)[0] != neighborOp[0]
              && nOpDimList.get(0)[1] == 1) {
              // If this neighbor sends data to another neighbor
              // Remove receiving on neighbor's neighbor
              // and do pairing with this node
              modifiedNeighbors.add(nOpDimList.get(0)[0]);
              List<int[]> nnOpDimList =
                nodeMap.get(nOpDimList.get(0)[0]).getNeighborOpList().get(i);
              int j = 1;
              while (j < nnOpDimList.size()) {
                if (nnOpDimList.get(j)[0] == neighborID) {
                  break;
                }
                j++;
              }
              nnOpDimList.remove(j);
              // Set this neighbor as a pair.
              neighborOp[1] = 2;
              nOpDimList.get(0)[0] = nodeID;
              nOpDimList.get(0)[1] = 2;
            } else {
              nOpDimList.add(new int[] { nodeID, 0 });
            }
          } else if (neighborOp[1] == 2) {
            if (nOpDimList.get(0)[0] == -1 && nOpDimList.get(0)[1] == -1) {
              // If no pair or sending in original topology
              // No operation.
              // Set pairing at the end
            } else if (nOpDimList.get(0)[0] != neighborOp[0]
              && nOpDimList.get(0)[1] == 2) {
              // If this neighbor already has a pair
              // Change neighbor's neighbor to send only
              modifiedNeighbors.add(nOpDimList.get(0)[0]);
              List<int[]> nnOpDimList =
                nodeMap.get(nOpDimList.get(0)[0]).getNeighborOpList().get(i);
              nnOpDimList.get(0)[1] = 1;
              // Add "receive" on the neighbor
              nOpDimList.add(new int[] { nOpDimList.get(0)[0], 0 });
            } else if (nOpDimList.get(0)[0] != neighborOp[0]
              && nOpDimList.get(0)[1] == 1) {
              // If this neighbor sends data to another neighbor
              // Remove receiving on neighbor's neighbor
              modifiedNeighbors.add(nOpDimList.get(0)[0]);
              List<int[]> nnOpDimList =
                nodeMap.get(nOpDimList.get(0)[0]).getNeighborOpList().get(i);
              int j = 1;
              while (j < nnOpDimList.size()) {
                if (nnOpDimList.get(j)[0] == neighborID) {
                  break;
                }
                j++;
              }
              nnOpDimList.remove(j);
            }
            // Set this neighbor as a pair.
            nOpDimList.get(0)[0] = nodeID;
            nOpDimList.get(0)[1] = 2;
          }
        }
      }
      neighborOpList.add(neighborOpDimList);
      // Update and go to next dimension
      moduloBase *= 2;
      moduloBaseHalf = moduloBase / 2;
      moduloResult = -1;
      neighborID = -1;
      neighborNode = null;
      neighborOp = null;
    }
    // Set neighbor list of this node
    return modifiedNeighbors;
  }

  private int updateDimensionLevel(int maxNodeID) {
    // Get the ceiling integer of log2(max+1)
    double doubleD = Math.log(maxNodeID + 1) / Math.log(2);
    int dimLevel = (int) doubleD;
    if (doubleD > dimLevel) {
      dimLevel++;
    }
    if (dimLevel == 0) {
      dimLevel = 1;
    }
    return dimLevel;
  }

  /**
   * Search if there is other neighbor available. The original node may be dead
   * or doesn't exist under the current hyper cube size. If we cannot get the
   * alternative node. -1 means no target exist for sending or pairing due to
   * the topology size. -2 means no target exist for sending or pairing due to
   * the dead zone.
   * 
   * @param nodeID
   * @param originalNeighborID
   * @param maxNodeID
   * @param moduloBase
   * @param nodeMap
   * @return
   */
  private int findAltNeighborInZone(int nodeID, int originalNeighborID,
    int maxNodeID, int moduloBase, int dimID,
    Map<Integer, HyperCubeNode> nodeMap) {
    int downStartID = originalNeighborID - moduloBase;
    int upStartID = originalNeighborID + moduloBase;
    int searchCount = 0;
    int nNodeID = -1;
    boolean downFound = false;
    boolean upFound = false;
    int downOpCount = -1;
    int upOpCount = -1;
    HyperCubeNode neighborNode = null;
    // Search downward
    while (downStartID >= 0) {
      neighborNode = nodeMap.get(downStartID);
      // We don't check if the node is running
      if (neighborNode != null) {
        downFound = true;
        nNodeID = downStartID;
        downOpCount = neighborNode.getNeighborOpList().get(dimID).size();
        break;
      }
      // Move to next candidate
      downStartID -= moduloBase;
      searchCount++;
    }
    // Search upward
    while (upStartID <= maxNodeID) {
      neighborNode = nodeMap.get(upStartID);
      // We don't check if the node is running
      if (neighborNode != null) {
        upFound = true;
        upOpCount = neighborNode.getNeighborOpList().get(dimID).size();
        if (downFound) {
          if (upOpCount < downOpCount) {
            nNodeID = upStartID;
          }
        } else {
          nNodeID = upStartID;
        }
        break;
      }
      // Move to next candidate
      upStartID += moduloBase;
      searchCount++;
    }
    if (downFound || upFound) {
      return nNodeID;
    }
    boolean isNeighborIDValid =
      (originalNeighborID >= 0 && originalNeighborID <= maxNodeID) ? true
        : false;
    // No neighboring needed
    if (!isNeighborIDValid && searchCount == 0) {
      return -1;
    }
    // We reach a dead zone
    // There should be valid nodes for neighboring, but they don't exisit!
    return -2;
  }

  private List<Integer> findAllOtherNodesInZone(int nodeID, int moduloBase,
    int maxNodeID, Map<Integer, HyperCubeNode> nodeMap) {
    List<Integer> allNodes = new ArrayList<Integer>();
    int upStartID = nodeID + moduloBase;
    int downStartID = nodeID - moduloBase;
    HyperCubeNode node = null;
    // Search upward
    while (upStartID <= maxNodeID) {
      node = nodeMap.get(upStartID);
      // We don't check if the node is running
      if (node != null) {
        allNodes.add(upStartID);
      }
      upStartID += moduloBase;
    }
    // Search downward
    while (downStartID >= 0) {
      node = nodeMap.get(downStartID);
      // We don't check if the node is running
      if (node != null) {
        allNodes.add(upStartID);
      }
      downStartID -= moduloBase;
    }
    return allNodes;
  }

  @Override
  public synchronized void removeTask(final String taskID) {
    int nodeID = taskMap.get(taskID);
    int maxNodeID = hyperCubeNodeMaxID;
    removeHyperCubeNode(taskID, nodeID, maxNodeID, taskMap, nodeMap);
  }

  /**
   * Delete a hyper cube node from the topology. The current topology is
   * considered as a valid toplogy (which can work).
   * 
   * @param taskID
   * @param nodeID
   * @param maxNodeID
   * @param taskMap
   * @param nodeMap
   * @return
   */
  private Set<Integer> removeHyperCubeNode(final String taskID,
    final int nodeID, final int maxNodeID, Map<String, Integer> taskMap,
    Map<Integer, HyperCubeNode> nodeMap) {
    taskMap.remove(taskID);
    HyperCubeNode node = nodeMap.remove(nodeID);
    // Make this ID be available to reuse
    freeNodeIDs.add(node.getNodeID());
    Set<Integer> modifiedNodes = new HashSet<Integer>();
    // Get deleted node;s neighbor op list
    List<List<int[]>> opList = node.getNeighborOpList();
    int moduloBase = 2;
    for (int i = 0; i < opList.size(); i++) {
      // For each dimension level
      for (int[] op : opList.get(i)) {
        // If it is not {-2, -1} or {-1, -1}
        if (op[0] >= 0 && op[1] != -1) {
          // Update each neighbor
          Set<Integer> modifiedNeighbors =
            updateNeighborList(nodeMap.get(op[0]), nodeID, moduloBase, i,
              maxNodeID, nodeMap);
          modifiedNodes.addAll(modifiedNeighbors);
        }
      }
      moduloBase *= 2;
    }
    return modifiedNodes;
  }

  /**
   * In a existing topology (which is valid), Update a node's neighbor list with
   * knowing the failure of one of its neighbor.
   * 
   */
  private Set<Integer> updateNeighborList(HyperCubeNode node, int deadNodeID,
    int moduloBase, int dimID, int maxNodeID,
    Map<Integer, HyperCubeNode> nodeMap) {
    int nodeID = node.getNodeID();
    // Records nodes which have been modified during update
    Set<Integer> modifiedNodes = new HashSet<Integer>();
    modifiedNodes.add(nodeID);
    List<List<int[]>> neighborOpList = node.getNeighborOpList();
    // The original op at this level is related to the dead node
    List<int[]> neighborOpDimList = neighborOpList.get(dimID);
    int[] op = null;
    // The first op is a "sending" or a "pairing"
    op = neighborOpDimList.get(0);
    if (op[0] == deadNodeID) {
      // Find the alternative neighbor
      int neighborID =
        findAltNeighborInZone(nodeID, deadNodeID, maxNodeID, moduloBase, dimID,
          nodeMap);
      // Find alternative neighbor from dead node's zone
      // If cannot find the neighbor, then set neighbor ID to -1 or -2
      if (neighborID < 0) {
        op[0] = neighborID;
        op[1] = -1;
      } else {
        // Get neighbor's op list at this dimension
        List<int[]> nOpDimList =
          nodeMap.get(neighborID).getNeighborOpList().get(dimID);
        int[] nOp = nOpDimList.get(0);
        // If this alternative neighbor is sending to the node,
        // try to remove the receiving and create a pair.
        // Though we create a pair here, in adding tasks, we try to replace the
        // pair with the real pairing. The real pairing is created through id
        // calculation. Because it won't overlap, once it exists, we remove the
        // temproary pairing caused by node deletion.
        if (nOp[0] == nodeID && nOp[1] == 1) {
          int rmOpIdex = -1;
          // Check your own ops, remove receiving
          int i = 1;
          while (i < neighborOpDimList.size()) {
            if (neighborOpDimList.get(i)[0] == neighborID) {
              rmOpIdex = i;
            }
            i++;
          }
          neighborOpDimList.remove(rmOpIdex);
          op[0] = neighborID;
          op[1] = 2;
          nOp[0] = nodeID;
          nOp[1] = 2;
        } else {
          // If it pairs with someone else or just do sending
          op[0] = neighborID;
          op[1] = 1;
          // Add receiving
          nOpDimList.add(new int[] { nodeID, 0 });
        }
      }
      modifiedNodes.add(neighborID);
    } else {
      // Dead node isn't seen in the first op,
      // It is only in "receive" ops, so just remove them.
      List<Integer> remvOpIndexList = new ArrayList<Integer>();
      for (int i = 1; i < neighborOpDimList.size(); i++) {
        op = neighborOpDimList.get(i);
        if (op[0] == deadNodeID && op[1] == 0) {
          // System.out.println("" + op[0] + " " + op[1] + " " + i);
          remvOpIndexList.add(i);
        }
      }
      // Remove from the last index
      for (int i = remvOpIndexList.size() - 1; i >= 0; i--) {
        neighborOpDimList.remove(remvOpIndexList.get(i).intValue());
      }
    }
    return modifiedNodes;
  }

  @Override
  public synchronized void setFailed(String taskID) {
    // Invoked by TopologyFailedTaskHandler when task is failed
    System.out.println(taskID + " fails.");
    // Once set failure,
    // 1. Tries to tell its neighbor nodes
    // that there is failure of the node of the task ID.
    // 2. Once the node gets the failure message, ack with the iteration it
    // notice the failure
    // 3. driver checks iteration and see if there is iteration lower than this
    // failure
    // 4. if yes, update the topology, and send a new topology, if no,send
    // message to ask the nodes to continue forwarding the failure message
    
    if (!isInitialized.get()) {
      // If it is not initialized
      // currently we don't do anything
      numRunningTasks.decrementAndGet();
    } else {
      // Add to failed tasks
      failedTasks.add(taskID);
      if (failedTaskNeighbors.contains(taskID)) {
        failedTaskNeighbors.remove(taskID);
      }
      int nodeID = taskMap.get(taskID);
      HyperCubeNode node = nodeMap.get(nodeID);
      List<List<int[]>> opList = node.getNeighborOpList();
      // Add to failed task neighbors
      // Add to failure map
      // If not exist, send failure message to each node
      boolean isFailureSent = false;
      for (List<int[]> opDimList : opList) {
        for (int[] op : opDimList) {
          if (op[0] >= 0 && op[1] >= 0) {
            // If both don't contain this task ID
            if (!failedTaskNeighbors.contains(nodeMap.get(op[0]).getTaskID())
              && !failedTasks.contains(nodeMap.get(op[0]).getTaskID())) {
              // Send failure message
              // If the node is failed, don't send
              // If the node has got a message, don't send again
              sendMsg(Utils.bldVersionedGCM(groupName, operName,
                Type.SourceDead, driverID, iteration.get(), nodeMap.get(op[0])
                  .getTaskID(), iteration.get(), EmptyByteArr));
              failedTaskNeighbors.add(nodeMap.get(op[0]).getTaskID());
              isFailureSent = true;
            } 
          }
        }
      }
      if (!isFailureSent) {
        // If no new messages sent out,
        // we need to worry about if processBeighborFailure could miss
        // the last failure report, check again.
        // if the condition is met, process neighbor failure.
        processNeighborFailure();
      }
    }
  }

  @Override
  public synchronized void setRunning(String taskID) {
    // Invoked by TopologyRunningTaskHandler when task is running
    if (!isInitialized.get()) {
      int nodeID = taskMap.get(taskID);
      HyperCubeNode node = nodeMap.get(nodeID);
      if (node == null) {
        System.out.println(taskID + " does not exist");
        LOG.warning(getQualifiedName() + taskID + " does not exist");
      }
      node.setRunning();
      if (numRunningTasks.incrementAndGet() == numTasks) {
        isInitialized.compareAndSet(false, true);
        initializeTopology(nodeMap);
      }
    }
  }

  private void initializeTopology(Map<Integer, HyperCubeNode> nodeMap) {
    HyperCubeNode node = null;
    for (Entry<Integer, HyperCubeNode> entry : nodeMap.entrySet()) {
      node = entry.getValue();
      byte[] bytes;
      try {
        bytes = encodeNodeTopologyToBytes(node, nodeMap);
        // Currently we try to build message directly on the original message
        // builder. It could be confusing, here we set source version and dest
        // version to be the same.
        System.out.println("Send topology setup msg to " + node.getTaskID()
          + " " + node.getTaskID());
        sendMsg(Utils.bldVersionedGCM(groupName, operName, Type.TopologySetup,
          driverID, iteration.get(), node.getTaskID(), iteration.get(), bytes));
      } catch (IOException e) {
        e.printStackTrace();
        LOG.log(Level.WARNING, "", e);
      }
    }
  }

  private byte[] encodeNodeTopologyToBytes(HyperCubeNode node,
    Map<Integer, HyperCubeNode> nodeMap) throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(bout);
    node.write(dout);
    // We add a map between node id and task id
    Map<Integer, String> nodeTaskMap =
      getOpTaskMap(node.getNeighborOpList(), nodeMap);
    dout.writeInt(nodeTaskMap.size());
    for (Entry<Integer, String> entry : nodeTaskMap.entrySet()) {
      dout.writeInt(entry.getKey());
      dout.writeUTF(entry.getValue());
    }
    return bout.toByteArray();
  }

  /**
   * Remember that when we do this, we are in the lock zone in set running.
   * 
   * @param opList
   * @return
   */
  private Map<Integer, String> getOpTaskMap(List<List<int[]>> opList,
    Map<Integer, HyperCubeNode> nodeMap) {
    Map<Integer, String> nodeTaskMap = new HashMap<Integer, String>();
    for (List<int[]> opDimList : opList) {
      for (int[] op : opDimList) {
        if (op[0] >= 0) {
          nodeTaskMap.put(op[0], nodeMap.get(op[0]).getTaskID());
        }
      }
    }
    return nodeTaskMap;
  }

  private void sendMsg(final GroupCommMessage gcm) {
    final String srcId = gcm.getSrcid();
    final Type msgType = gcm.getType();
    System.out.println(getQualifiedName() + ": Sending " + msgType
      + " msg from " + srcId);
    senderStage.onNext(gcm);
  }

  @Override
  public synchronized void processMsg(final GroupCommMessage msg) {
    // Invoked by TopologyMessageHandler when message arrives
    // We ignore TopologyChanges and UpdateTopology two messages
    // Assume nodes won't send these two types of messages
    // For broadcast and reduce operator, master task send these two types of
    // messages to the driver
    System.out.println(getQualifiedName() + "processing " + msg.getType()
      + " from " + msg.getSrcid());
    if (msg.getType().equals(Type.SourceDead)) {
      System.out.println("Get failure report: version: " + msg.getSrcVersion()
        + ", id: " + msg.getSrcid());
      failureReports.put(msg.getSrcid(), msg.getSrcVersion());
      processNeighborFailure();
    }
  }

  private void processNeighborFailure() {
    // Check if the reports include all the failed neighbors
    boolean allContained = true;
    int minVersion = Integer.MAX_VALUE;
    int maxVersion = Integer.MIN_VALUE;
    Integer taskVersion = null;
    for (String taskID : failedTaskNeighbors) {
      System.out.println("Failed task neighbor: " + taskID);
      taskVersion = failureReports.get(taskID);
      if (taskVersion != null) {
        if (minVersion > taskVersion.intValue()) {
          minVersion = taskVersion.intValue();
        }
        if (maxVersion < taskVersion.intValue()) {
          maxVersion = taskVersion.intValue();
        }
      } else {
        allContained = false;
        break;
      }
    }
    if (allContained) {
      // If yes,
      // Calculate which iteration the new topology is for
      // I doubt if the code above is correct
      // because the node which has more iterations may
      // have already exchanged data from the dead node
      // if (minVersion < maxVersion) {
      // iteration.set(maxVersion);
      // } else if (minVersion == maxVersion) {
      // iteration.set(maxVersion + 1);
      // }
      iteration.set(maxVersion + 1);
      // Send new topology and mark the iteration
      for (String taskID : failedTasks) {
        int nodeID = taskMap.get(taskID);
        int maxNodeID = hyperCubeNodeMaxID;
        removeHyperCubeNode(taskID, nodeID, maxNodeID, taskMap, nodeMap);
      }
      for (String taskID : failedTaskNeighbors) {
        // Send new topology
        try {
          HyperCubeNode node = nodeMap.get(taskMap.get(taskID));
          byte[] bytes = encodeNodeTopologyToBytes(node, nodeMap);
          System.out.println("Send topology update msg to " + node.getTaskID()
            + " with version " + iteration.get());
          LOG
            .log(Level.INFO, "Send topology update msg to " + node.getTaskID());
          sendMsg(Utils.bldVersionedGCM(groupName, operName,
            Type.UpdateTopology, driverID, iteration.get(), node.getTaskID(),
            iteration.get(), bytes));
        } catch (IOException e) {
          e.printStackTrace();
          LOG.log(Level.WARNING, "", e);
        }
      }
      // Clean failure related data structures
      failureReports.clear();
      failedTasks.clear();
      failedTaskNeighbors.clear();
    }
  }

  /**
   * @return
   */
  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName)
      + " - ";
  }

  void printHyperCube() {
    System.out.println("Print Hypercube");
    HyperCubeNode node = null;
    for (Entry<Integer, HyperCubeNode> entry : nodeMap.entrySet()) {
      node = entry.getValue();
      List<List<int[]>> opList = entry.getValue().getNeighborOpList();
      StringBuffer sb1 = new StringBuffer();
      StringBuffer sb2 = new StringBuffer();
      sb1.append(node.getNodeID() + ":|");
      sb2.append(node.getNodeID() + ":|");
      for (List<int[]> opDimList : opList) {
        for (int[] op : opDimList) {
          sb1.append(op[0] + " ");
          sb2.append(op[1] + " ");
        }
        sb1.append("|");
        sb2.append("|");
      }
      System.out.println(sb1);
      System.out.println(sb2);
    }
  }

  public static void main(String[] args) {
    HyperCubeTopology topo = new HyperCubeTopology(null, null, null, null, 5);
    topo.addTask("task-0");
    topo.addTask("task-1");
    topo.addTask("task-2");
    topo.addTask("task-3");
    topo.addTask("task-4");
    topo.addTask("task-5");
    topo.addTask("task-6");
    topo.addTask("task-7");
    // topo.addTask("task-8");
    // topo.printHyperCube();
    // topo.removeTask("task-0");
    // topo.printHyperCube();
    // topo.removeTask("task-1");
    // topo.printHyperCube();
    // topo.removeTask("task-2");
    // topo.removeTask("task-3");
    topo.printHyperCube();
    // topo.addTask("task-1", 1);
    // topo.addTask("task-2", 2);
    // topo.printHyperCube();
  }
}
