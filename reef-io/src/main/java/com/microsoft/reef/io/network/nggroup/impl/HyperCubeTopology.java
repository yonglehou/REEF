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
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
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
 * from FlatTopology.
 * 
 * addTask, setRunning, setFailed, and removeTask, 4 methods could be invoked
 * simultaneously. Make them "synchronized" to maintain the consistency of the
 * topology.
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

  // New data structures for hype cube topology
  private class TopoStruct {
    /**
     * This is used to generate hypercube ID (starts from 0, initialized as -1)
     * for each task.This is also used to track the max ID in the hyper cube.
     */
    private int hyperCubeNodeMaxID = -1;
    /**
     * The dimension level or the total number dimensions of the current hyper
     * cube (starts from 1, initialized as 0). This is calculated base on the
     * max ID of the hyper cube. See if we need to make this variable atomic.
     */
    private int dimensionLevel = 0;
    /** This is the mapping between task ID and hypercube node ID */
    private final Map<String, Integer> taskMap = new TreeMap<>();
    /** This is the mapping between hype cube ID and hyper cube node */
    private final Map<Integer, HyperCubeNode> nodeMap = new TreeMap<>();
    /** List of node ids which from failed nodes and make them be reusable */
    private final LinkedList<Integer> freeNodeIDs = new LinkedList<Integer>();
  }

  private TopoStruct activeTopo = new TopoStruct();

  /**
   * When the number of running tasks arrives at the minimum number required,
   * set the topology as initialized
   */
  private final AtomicBoolean isInitialized = new AtomicBoolean(false);
  /** The total number of tasks initialized */
  private final int minInitialTasks;
  private final AtomicInteger numRunningInitialTasks = new AtomicInteger(0);
  /** Iteration is used to track the iteration number of allreduce operation */
  // Because the version id could be used as iteration id for messages
  // transmitted between tasks and the messages between driver and tasks use
  // version id 0, we start iteration with 1.
  private final AtomicInteger iteration = new AtomicInteger(1);

  /** This is used to collect all new task ids */
  private final Set<String> newTasks = new HashSet<>();
  private final Set<String> newTaskNeighbors = new HashSet<>();
  /** This is used to collect all fail task ids */
  private final Set<String> failedTaskNeighbors = new HashSet<>();
  /** This is used to collect all reports about neighbor additions and deletions */
  private final Map<String, Integer> taskReports = new HashMap<>();
  private final AtomicBoolean isFailed = new AtomicBoolean(false);
  private TopoStruct sandBox = null;

  public HyperCubeTopology(final EStage<GroupCommMessage> senderStage,
    final Class<? extends Name<String>> groupName,
    final Class<? extends Name<String>> operatorName, final String driverID,
    final int numberOfTasks) {
    this.senderStage = senderStage;
    this.groupName = groupName;
    this.operName = operatorName;
    this.driverID = driverID;
    this.minInitialTasks = numberOfTasks;
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
    // The topology configuration binds with the sender/receiver client
    // implementation!
    // Fix the number of version to 0
    final int version = 0;
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
    // However, added but not-running tasks don't have any affect to the toplogy
    // we move node adding when the tasks are running.
  }

  /**
   * Used only in unit test
   * 
   * @param taskID
   */
  private Set<Integer> newTask(TopoStruct topo, String taskID) {
    if (topo == null) {
      topo = activeTopo;
    }
    int nodeID = -1;
    if (!topo.freeNodeIDs.isEmpty()) {
      // Failed tasks are removed from the topology directly if the topology is
      // not initialized. The node IDs which failed tasks used to occupy go to
      // free node IDs.
      nodeID = topo.freeNodeIDs.poll();
    } else {
      // Increase the current max ID to get next ID and new max ID
      // New nodes are always added with IDs next larger than the current
      // max
      topo.hyperCubeNodeMaxID++;
      // Calculate the current dimension level based on the new max ID
      topo.dimensionLevel = updateDimensionLevel(topo.hyperCubeNodeMaxID);
      nodeID = topo.hyperCubeNodeMaxID;
    }
    System.out.println(getQualifiedName() + "Adding hyper cube node " + taskID
      + " with node id " + nodeID + " at the dimension level: "
      + topo.dimensionLevel);
    return addHyperCubeNode(taskID, nodeID, topo.hyperCubeNodeMaxID,
      topo.dimensionLevel, topo.taskMap, topo.nodeMap);
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
   * Used only in unit test
   * 
   * @param taskID
   */
  @SuppressWarnings("unused")
  private Set<Integer> newTask(TopoStruct topo, String taskID, int nodeID) {
    if (topo == null) {
      topo = activeTopo;
    }
    if (!topo.nodeMap.containsKey(nodeID)) {
      return addHyperCubeNode(taskID, nodeID, topo.hyperCubeNodeMaxID,
        topo.dimensionLevel, topo.taskMap, topo.nodeMap);
    }
    return null;
  }

  /**
   * When a new task is added, invoke this method to add a task to a node in the
   * hypercube. This method is required to be synchronized because it may be
   * invoked by multiple threads.
   * 
   */
  private Set<Integer> addHyperCubeNode(String taskID, int nodeID,
    int maxNodeID, int dimLevel, Map<String, Integer> taskMap,
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
   * This method returns the ids of nodes modified.
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
  public synchronized void setRunning(String taskID) {
    // Invoked by TopologyRunningTaskHandler when task is running
    System.out.println(taskID + " is running.");
    // We add the number of running tasks.
    this.numRunningInitialTasks.incrementAndGet();
    if (!isInitialized.get()) {
      newTask(activeTopo, taskID);
      if (this.numRunningInitialTasks.get() == this.minInitialTasks) {
        initializeTopology(activeTopo);
        isInitialized.set(true);
        System.out.println("Hypercube topology is initialized!");
      }
    } else {
      // Once the topology is initialized,
      // copy the current active topology to a sand box (maybe there is one, if
      // sandbox is not null), apply adding and see which neighbor nodes are
      // modified
      if (sandBox == null) {
        System.out.println("Copy active toopology to sandbox.");
        sandBox = copyTopology(activeTopo);
      }
      Set<Integer> modifiedNeighbors = newTask(sandBox, taskID);
      System.out.println("Print active Hypercube");
      printHyperCube(activeTopo);
      System.out.println("Print Hypercube in sandbox");
      printHyperCube(sandBox);
      // Add this task to new tasks
      // Even this new task has the same ID which is used before
      // the original task with the same ID must be set failed and then be added
      // and set running again. So no need to worry about if this task ID could
      // be in other sets.
      newTasks.add(taskID);
      // If these nodes are not in failedTaskNeighbors,
      // add to newTaskNeighbors and send source add message
      boolean isAdditionSent = false;
      for (int mNodeID : modifiedNeighbors) {
        String mTaskID = sandBox.nodeMap.get(mNodeID).getTaskID();
        if (!failedTaskNeighbors.contains(mTaskID)
          && !newTaskNeighbors.contains(mTaskID) && !newTasks.contains(mTaskID)) {
          System.out.println("New task neighbor: " + mTaskID);
          // The version number is always 0.
          sendMsg(Utils.bldVersionedGCM(groupName, operName, Type.SourceAdd,
            driverID, 0, mTaskID, 0, EmptyByteArr));
          newTaskNeighbors.add(mTaskID);
          newTaskNeighbors.add(mTaskID);
          isAdditionSent = true;
        }
      }
      // If no new message sent, try to see if we can process the task reports
      // received and update topology.
      if (!isAdditionSent) {
        processNeighborUpdate();
      }
    }
  }

  private void initializeTopology(TopoStruct topo) {
    HyperCubeNode node = null;
    int ite = iteration.get();
    for (Entry<Integer, HyperCubeNode> entry : topo.nodeMap.entrySet()) {
      node = entry.getValue();
      System.out.println("Send topology setup msg to " + node.getTaskID());
      try {
        // The base iteration and the new iteration are the same.
        sendTopology(node, topo, Type.TopologySetup, ite, ite, false);
      } catch (IOException e) {
        e.printStackTrace();
        LOG.log(Level.WARNING, "", e);
      }
    }
  }

  private void sendTopology(HyperCubeNode node, TopoStruct topo, Type msgType,
    int baseIteration, int newIteration, boolean isFailed) throws IOException {
    byte[] bytes =
      encodeNodeTopologyToBytes(node, topo.nodeMap, baseIteration,
        newIteration, isFailed);
    // Currently we try to build message directly on the original message
    // builder. The version number is always 0.
    sendMsg(Utils.bldVersionedGCM(groupName, operName, msgType, driverID, 0,
      node.getTaskID(), 0, bytes));
  }

  private TopoStruct copyTopology(TopoStruct topo) {
    TopoStruct sandbox = new TopoStruct();
    sandbox.hyperCubeNodeMaxID = topo.hyperCubeNodeMaxID;
    sandbox.dimensionLevel = topo.dimensionLevel;
    for (int i = 0; i < topo.freeNodeIDs.size(); i++) {
      sandbox.freeNodeIDs.add(topo.freeNodeIDs.get(i).intValue());
    }
    for (Entry<String, Integer> entry : topo.taskMap.entrySet()) {
      // String is immutable, put directly.
      sandbox.taskMap.put(entry.getKey(), entry.getValue().intValue());
    }
    for (Entry<Integer, HyperCubeNode> entry : topo.nodeMap.entrySet()) {
      // Every node object is copied and is a new object
      sandbox.nodeMap.put(entry.getKey(), entry.getValue().clone());
    }
    return sandbox;
  }

  private byte[] encodeNodeTopologyToBytes(HyperCubeNode node,
    Map<Integer, HyperCubeNode> nodeMap, int baseIteration, int newIteration,
    boolean isFailed) throws IOException {
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
    dout.writeInt(baseIteration);
    dout.writeInt(newIteration);
    dout.writeBoolean(isFailed);
    dout.flush();
    dout.close();
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

  private void sendMsg(GroupCommMessage gcm) {
    System.out.println(getQualifiedName() + ": sendMsg " + gcm.getType()
      + " from " + gcm.getSrcid() + " to " + gcm.getDestid());
    senderStage.onNext(gcm);
  }

  @Override
  public synchronized void removeTask(final String taskID) {

  }

  private Set<Integer> deleteTask(TopoStruct topo, String taskID) {
    if (topo == null) {
      topo = activeTopo;
    }
    // Cannot find such task
    if (!topo.taskMap.containsKey(taskID)) {
      return null;
    }
    int nodeID = topo.taskMap.get(taskID);
    int maxNodeID = topo.hyperCubeNodeMaxID;
    return removeHyperCubeNode(taskID, nodeID, maxNodeID, topo.freeNodeIDs,
      topo.taskMap, topo.nodeMap);
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
  private Set<Integer> removeHyperCubeNode(String taskID, int nodeID,
    int maxNodeID, List<Integer> freeNodeIDs, Map<String, Integer> taskMap,
    Map<Integer, HyperCubeNode> nodeMap) {
    taskMap.remove(taskID);
    HyperCubeNode node = nodeMap.remove(nodeID);
    // Make this ID be available to reuse
    freeNodeIDs.add(nodeID);
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
   * knowing the failure of one of its neighbor. Return the neighbors modified.
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
        // For neighbor ID < 0 are not recorded
        modifiedNodes.add(neighborID);
      }
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
  public synchronized void setFailed(final String taskID) {
    // Invoked by TopologyFailedTaskHandler when task is failed
    System.out.println(taskID + " fails.");
    // We decrease the number of running tasks.
    numRunningInitialTasks.decrementAndGet();
    if (!isInitialized.get()) {
      deleteTask(activeTopo, taskID);
    } else {
      // Once the topology is initialized,
      // copy the current active topology to a sand box (maybe there is one, if
      // sandbox is not null), apply adding and see which neighbor nodes are
      // modified
      if (sandBox == null) {
        sandBox = copyTopology(activeTopo);
      }
      // There are failed tasks in the current topology
      isFailed.set(true);
      System.out.println("Print active Hypercube.");
      printHyperCube(activeTopo);
      System.out.println("Print sandbox Hypercube.");
      printHyperCube(sandBox);
      // See if this task is in newTasks.
      // if yes, remove.
      if (newTasks.contains(taskID)) {
        newTasks.remove(taskID);
      }
      // See if this task is in newTaskNeighbors.
      // if yes, remove.
      if (newTaskNeighbors.contains(taskID)) {
        newTaskNeighbors.remove(taskID);
      }
      // See if this task is in failedTaskNeighbors.
      // if yes, remove.
      if (failedTaskNeighbors.contains(taskID)) {
        failedTaskNeighbors.remove(taskID);
      }
      // We try to notify all the rest tasks that there is a failure.
      // And then coordinate "recover".
      Set<String> modifiedNeighbors = new HashSet<>();
      for (Entry<Integer, HyperCubeNode> entry : sandBox.nodeMap.entrySet()) {
        modifiedNeighbors.add(entry.getValue().getTaskID());
      }
      modifiedNeighbors.remove(taskID);
      // Update the topology in sandbox after the failure
      deleteTask(sandBox, taskID);
      boolean isFailureSent = false;
      for (String mTaskID : modifiedNeighbors) {
        // If neither don't contain this task ID
        if (!newTasks.contains(mTaskID) && !newTaskNeighbors.contains(mTaskID)
          && !failedTaskNeighbors.contains(mTaskID)) {
          System.out.println("Failed task neighbor: " + mTaskID);
          // Send failure message
          // The version number is always 0.
          sendMsg(Utils.bldVersionedGCM(groupName, operName, Type.SourceDead,
            driverID, 0, mTaskID, 0, EmptyByteArr));
          failedTaskNeighbors.add(mTaskID);
          isFailureSent = true;
        }
      }
      if (!isFailureSent) {
        // If no new messages sent out,
        // we need to check again if we have got all task reports,
        // and if we can process task failure.
        processNeighborUpdate();
      }
    }
  }

  @Override
  public synchronized void processMsg(final GroupCommMessage msg) {
    // Invoked by TopologyMessageHandler when message arrives
    // We ignore TopologyChanges and UpdateTopology two messages
    // Assume nodes won't send these two types of messages
    int iteration = getIterationInBytes(Utils.getData(msg));
    System.out.println(getQualifiedName() + "processing " + msg.getType()
      + " from " + msg.getSrcid() + " with iteration " + iteration);
    if (msg.getType().equals(Type.SourceDead)) {
      taskReports.put(msg.getSrcid(), iteration);
      processNeighborUpdate();
    } else if (msg.getType().equals(Type.SourceAdd)) {
      taskReports.put(msg.getSrcid(), iteration);
      processNeighborUpdate();
    }
  }
  
  private int getIterationInBytes(byte[] bytes) {
    ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
    DataInputStream din = new DataInputStream(bin);
    int iteration = 0;
    try {
      iteration = din.readInt();
      din.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return iteration;
  }

  private void processNeighborUpdate() {
    // Check if all the task reports include all the failed neighbors
    System.out.println("Process neighbor update.");
    if (this.numRunningInitialTasks.get() < this.minInitialTasks) {
      // We don't process the neighbor update if we haven't got the
      // minimum number of running tasks.
      System.out.println("The current number of tasks is lower than"
        + " the number of running tasks required.");
      return;
    }
    // We collect reports from new task neighbors and failed task neighbors
    // For new tasks waiting for topology setup, they don't have task reports.
    boolean allContained = true;
    int minIteration = Integer.MAX_VALUE;
    int maxIteration = Integer.MIN_VALUE;
    Integer taskVersion = null;
    for (String taskID : failedTaskNeighbors) {
      System.out.println("Current failed task neighbor: " + taskID);
      taskVersion = taskReports.get(taskID);
      if (taskVersion != null) {
        if (minIteration > taskVersion.intValue()) {
          minIteration = taskVersion.intValue();
        }
        if (maxIteration < taskVersion.intValue()) {
          maxIteration = taskVersion.intValue();
        }
      } else {
        allContained = false;
        break;
      }
    }
    if (allContained) {
      // Check if all the task reports include all the new task neighbors
      for (String taskID : newTaskNeighbors) {
        System.out.println("Current new task neighbor: " + taskID);
        taskVersion = taskReports.get(taskID);
        if (taskVersion != null) {
          if (minIteration > taskVersion.intValue()) {
            minIteration = taskVersion.intValue();
          }
          if (maxIteration < taskVersion.intValue()) {
            maxIteration = taskVersion.intValue();
          }
        } else {
          allContained = false;
          break;
        }
      }
    }
    if (allContained) {
      // If yes, calculate which iteration the new topology is for
      activeTopo = sandBox;
      sandBox = null;
      // Set the internal agreed iteration number for updating the topology.
      int baseIteration = minIteration;
      int newIteration = maxIteration + 1;
      iteration.set(newIteration);
      // Because all tasks are blocked when there is failures.
      for (String taskID : failedTaskNeighbors) {
        // Send new topology
        HyperCubeNode node =
          activeTopo.nodeMap.get(activeTopo.taskMap.get(taskID));
        try {
          sendTopology(node, activeTopo, Type.UpdateTopology, baseIteration,
            newIteration, isFailed.get());
        } catch (IOException e) {
          e.printStackTrace();
          LOG.log(Level.WARNING, "", e);
        }
      }
      for (String taskID : newTaskNeighbors) {
        // Send new topology
        HyperCubeNode node =
          activeTopo.nodeMap.get(activeTopo.taskMap.get(taskID));
        try {
          sendTopology(node, activeTopo, Type.UpdateTopology, baseIteration,
            newIteration, isFailed.get());
        } catch (IOException e) {
          e.printStackTrace();
          LOG.log(Level.WARNING, "", e);
        }
      }
      for (String taskID : newTasks) {
        // Send new topology
        HyperCubeNode node =
          activeTopo.nodeMap.get(activeTopo.taskMap.get(taskID));
        try {
          sendTopology(node, activeTopo, Type.TopologySetup, baseIteration,
            newIteration, false);
        } catch (IOException e) {
          e.printStackTrace();
          LOG.log(Level.WARNING, "", e);
        }
      }
      // Clean related data structures
      taskReports.clear();
      failedTaskNeighbors.clear();
      newTaskNeighbors.clear();
      newTasks.clear();
      isFailed.set(false);
    }
  }

  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName)
      + " - ";
  }

  private void printHyperCube(TopoStruct topo) {
    if (topo == null) {
      topo = activeTopo;
    }
    HyperCubeNode node = null;
    for (Entry<Integer, HyperCubeNode> entry : topo.nodeMap.entrySet()) {
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
    topo.newTask(null, "task-0");
    topo.newTask(null, "task-1");
    topo.newTask(null, "task-2");
    topo.newTask(null, "task-3");
    topo.newTask(null, "task-4");
    topo.newTask(null, "task-5");
    topo.newTask(null, "task-6");
    topo.newTask(null, "task-7");
    // topo.addTask("task-8");
    // topo.printHyperCube();
    // topo.removeTask("task-0");
    // topo.printHyperCube();
    // topo.removeTask("task-1");
    // topo.printHyperCube();
    // topo.removeTask("task-2");
    // topo.removeTask("task-3");
    topo.printHyperCube(null);
    // topo.addTask("task-1", 1);
    // topo.addTask("task-2", 2);
    // topo.printHyperCube();
  }

  @Override
  public boolean isRunning(String taskId) {
    // This method is not invoked.
    System.out.println("Is topology running?");
    return false;
  }

  @Override
  public int getNodeVersion(String taskId) {
    // This method is used to check if the version of the message
    // from the sender is matched with the current topology version.
    // Always uses 0.
    return 0;
  }
}
