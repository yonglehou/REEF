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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

// New data structures for hype cube topology
class TopoStruct {
  /**
   * This is used to generate hypercube ID (starts from 0, initialized as -1)
   * for each task.This is also used to track the max ID in the hyper cube.
   */
  int hyperCubeNodeMaxID;
  /**
   * The dimension level or the total number dimensions of the current hyper
   * cube (starts from 1, initialized as 0). This is calculated base on the max
   * ID of the hyper cube. See if we need to make this variable atomic.
   */
  int dimensionLevel;
  /** This is the mapping between task ID and hypercube node ID */
  final Map<String, Integer> taskMap;
  /** This is the mapping between hype cube ID and hyper cube node */
  final Map<Integer, HyperCubeNode> nodeMap;
  /** List of node ids which from failed nodes and make them be reusable */
  final LinkedList<Integer> freeNodeIDs;

  TopoStruct() {
    hyperCubeNodeMaxID = -1;
    dimensionLevel = 0;
    taskMap = new TreeMap<>();
    nodeMap = new TreeMap<>();
    freeNodeIDs = new LinkedList<Integer>();
  }
}

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

  private TopoStruct activeTopo;
  private TopoStruct sandBox;
  /**
   * Because task ids are reused when restarting, track task version. We keep
   * every task id existed and update the version id when it comes back. The
   * content of the map doesn't refelect the topology.
   */
  private final ConcurrentMap<String, AtomicInteger> taskVersionMap;

  /**
   * When the number of running tasks arrives at the minimum number required,
   * set the topology as initialized
   */
  private final AtomicBoolean isInitialized;
  /** The total number of tasks initialized */
  private final int minInitialTasks;
  private final AtomicInteger numRunningTasks;
  /** Iteration is used to track the iteration number of allreduce operation */
  private final AtomicInteger iteration;

  /** This is used to collect all new task ids */
  private final Set<String> newTasks;
  private final Set<String> newTaskNeighbors;
  /** This is used to collect all fail task ids */
  private final Set<String> failedTaskNeighbors;
  /** This is used to collect all reports about neighbor additions and deletions */
  private final Map<String, Integer> taskReports;
  private final AtomicBoolean isFailed;

  public HyperCubeTopology(final EStage<GroupCommMessage> senderStage,
    final Class<? extends Name<String>> groupName,
    final Class<? extends Name<String>> operatorName, final String driverID,
    final int numberOfTasks) {
    this.senderStage = senderStage;
    this.groupName = groupName;
    this.operName = operatorName;
    this.driverID = driverID;

    activeTopo = new TopoStruct();
    sandBox = null;
    isInitialized = new AtomicBoolean(false);
    // this.minInitialTasks = numberOfTasks / 2 + 1;
    minInitialTasks = numberOfTasks;
    numRunningTasks = new AtomicInteger(0);
    iteration = new AtomicInteger(0);
    taskVersionMap = new ConcurrentHashMap<>();
    newTasks = new HashSet<>();
    newTaskNeighbors = new HashSet<>();
    failedTaskNeighbors = new HashSet<>();
    taskReports = new HashMap<>();
    isFailed = new AtomicBoolean(false);
  }

  @Override
  public void setRoot(final String rootId) {
    // No root id. We could assign a root id in future if we use this topology
    // to do broadcast or reduce.
  }

  @Override
  public String getRootId() {
    return null;
  }

  @Override
  public void setOperSpec(final OperatorSpec spec) {
    this.operatorSpec = spec;
  }

  @Override
  public synchronized Configuration getConfig(final String taskID) {
    // The topology configuration binds with the sender/receiver client
    // implementation!
    // When a task is failed, it will restart with the same taskID,
    // to separate the old task and the new task, we use version in
    // configuration with is agreed between driver and a task.
    final int version = getNodeVersion(taskID);
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
  public synchronized int getNodeVersion(String taskID) {
    return createNodeVersion(taskID).get();
  }

  private AtomicInteger createNodeVersion(String taskID) {
    AtomicInteger version = taskVersionMap.get(taskID);
    if (version == null) {
      version = new AtomicInteger(0);
      AtomicInteger oldVersion = taskVersionMap.putIfAbsent(taskID, version);
      if (oldVersion != null) {
        version = oldVersion;
      }
    }
    return version;
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
    printLog("Adding hyper cube node " + taskID + " with node id " + nodeID
      + " at the dimension level: " + topo.dimensionLevel);
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
      neighborOpDimList = new LinkedList<int[]>();
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
            nOpDimList = new LinkedList<int[]>();
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
            printLog("Node " + id + " in the zone where " + neighborID + " is.");
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
              int rmOpIndex = -1;
              for (int j = 1; j < nnOpDimList.size(); j++) {
                if (nnOpDimList.get(j)[0] == neighborID) {
                  rmOpIndex = j;
                  break;
                }
              }
              nnOpDimList.remove(rmOpIndex);
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
              // get neighbor's original paired neighbor
              int nnNodeID = nOpDimList.get(0)[0];
              modifiedNeighbors.add(nnNodeID);
              List<int[]> nnOpDimList =
                nodeMap.get(nnNodeID).getNeighborOpList().get(i);
              if (nnOpDimList.size() > 1) {
                // If this neighbor's neighbor has receiving
                int[] firstRecvOp = nnOpDimList.remove(1);
                // Pairing neighbor's neighbor with one node on receiving
                nnOpDimList.get(0)[0] = firstRecvOp[0];
                nnOpDimList.get(0)[1] = 2;
                // Modify neighbor's neighbor's neighbor
                modifiedNeighbors.add(firstRecvOp[0]);
                List<int[]> nnnOpDimList =
                  nodeMap.get(firstRecvOp[0]).getNeighborOpList().get(i);
                nnnOpDimList.get(0)[0] = nnNodeID;
                nnnOpDimList.get(0)[1] = 2;
              } else {
                // Change neighbor's neighbor to send only
                nnOpDimList.get(0)[1] = 1;
                // Add "receive" on the neighbor
                nOpDimList.add(new int[] { nnNodeID, 0 });
              }
            } else if (nOpDimList.get(0)[0] != neighborOp[0]
              && nOpDimList.get(0)[1] == 1) {
              // If this neighbor sends data to another neighbor
              // Remove receiving on neighbor's neighbor
              modifiedNeighbors.add(nOpDimList.get(0)[0]);
              List<int[]> nnOpDimList =
                nodeMap.get(nOpDimList.get(0)[0]).getNeighborOpList().get(i);
              int rmOpIndex = -1;
              for (int j = 1; j < nnOpDimList.size(); j++) {
                if (nnOpDimList.get(j)[0] == neighborID) {
                  rmOpIndex = j;
                  break;
                }
              }
              nnOpDimList.remove(rmOpIndex);
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
    List<Integer> allNodes = new LinkedList<Integer>();
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
        allNodes.add(downStartID);
      }
      downStartID -= moduloBase;
    }
    return allNodes;
  }

  @Override
  public synchronized void setRunning(String taskID) {
    // Invoked by TopologyRunningTaskHandler when task is running
    printLog(taskID + " is running.");
    // We add the number of running tasks.
    this.numRunningTasks.incrementAndGet();
    if (!isInitialized.get()) {
      newTask(activeTopo, taskID);
      if (this.numRunningTasks.get() == this.minInitialTasks) {
        initializeTopology(activeTopo, taskVersionMap);
        isInitialized.set(true);
        printLog("Hypercube topology is initialized!");
      }
    } else {
      // Once the topology is initialized,
      // copy the current active topology to a sand box (maybe there is one, if
      // sandbox is not null), apply adding and see which neighbor nodes are
      // modified
      if (sandBox == null) {
        printLog("Copy active toopology to sandbox.");
        sandBox = copyTopology(activeTopo);
      }
      Set<Integer> modifiedNeighbors = newTask(sandBox, taskID);
      printLog("Print active Hypercube");
      printHyperCube(activeTopo);
      printLog("Print Hypercube in sandbox");
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
          printLog("New task neighbor: " + mTaskID);
          // The version number is a number agreed bwtween driver-task
          // communication
          sendMsg(Utils.bldVersionedGCM(groupName, operName, Type.SourceAdd,
            driverID, getNodeVersion(mTaskID), mTaskID,
            getNodeVersion(mTaskID), EmptyByteArr));
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

  private void initializeTopology(TopoStruct topo,
    Map<String, AtomicInteger> taskVersionMap) {
    HyperCubeNode node = null;
    int ite = iteration.get();
    for (Entry<Integer, HyperCubeNode> entry : topo.nodeMap.entrySet()) {
      node = entry.getValue();
      try {
        // The base iteration and the new iteration are the same.
        sendTopology(node, topo, taskVersionMap, Type.TopologySetup, ite, ite,
          false);
      } catch (IOException e) {
        e.printStackTrace();
        LOG.log(Level.WARNING, "", e);
      }
    }
  }

  private void sendTopology(HyperCubeNode node, TopoStruct topo,
    Map<String, AtomicInteger> taskVersionMap, Type msgType, int baseIteration,
    int newIteration, boolean isFailed) throws IOException {
    byte[] bytes =
      encodeNodeTopologyToBytes(node, topo.nodeMap, taskVersionMap,
        baseIteration, newIteration, isFailed);
    // Currently we try to build message directly on the original message
    // builder. The version number is a number agreed by the driver and this
    // task.
    sendMsg(Utils.bldVersionedGCM(groupName, operName, msgType, driverID,
      getNodeVersion(node.getTaskID()), node.getTaskID(),
      getNodeVersion(node.getTaskID()), bytes));
  }

  private TopoStruct copyTopology(TopoStruct topo) {
    if (topo == null) {
      topo = this.activeTopo;
    }
    TopoStruct sandbox = new TopoStruct();
    if (topo == null) {
      printLog("Active topology is null");
    }
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
    Map<Integer, HyperCubeNode> nodeMap,
    Map<String, AtomicInteger> taskVersionMap, int baseIteration,
    int newIteration, boolean isFailed) throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(bout);
    node.write(dout);
    // Add mapping between node id, task id and task version
    Map<Integer, String> nodeTaskMap =
      getOpTaskMap(node.getNeighborOpList(), nodeMap);
    dout.writeInt(nodeTaskMap.size());
    for (Entry<Integer, String> entry : nodeTaskMap.entrySet()) {
      // Node ID
      dout.writeInt(entry.getKey());
      // Task ID
      dout.writeUTF(entry.getValue());
      // Task Version
      dout.writeInt(taskVersionMap.get(entry.getValue()).get());
    }
    dout.writeInt(baseIteration);
    dout.writeInt(newIteration);
    dout.writeBoolean(isFailed);
    dout.flush();
    dout.close();
    return bout.toByteArray();
  }

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
    printLog("Send msg " + gcm.getType() + " from " + gcm.getSrcid()
      + " with version " + gcm.getSrcVersion() + " to " + gcm.getDestid()
      + " with version " + gcm.getVersion());
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
      if (op[1] == 2 && neighborOpDimList.size() > 1) {
        // If op[1] == 2 and neighborOpDimList.size() > 1
        // In this case, we need to modify pairing to sending only.
        // If there is a receiving op there, we set pairing to this node
        int[] firstRecvOp = neighborOpDimList.remove(1);
        // Get the neighbor's op list at this dimension
        modifiedNodes.add(firstRecvOp[0]);
        List<int[]> nOpDimList =
          nodeMap.get(firstRecvOp[0]).getNeighborOpList().get(dimID);
        int[] nOp = nOpDimList.get(0);
        nOp[0] = nodeID;
        nOp[1] = 2;
        op[0] = firstRecvOp[0];
        op[1] = 2;
      } else {
        // Find the alternative neighbor
        int neighborID =
          findAltNeighborInZone(nodeID, deadNodeID, maxNodeID, moduloBase,
            dimID, nodeMap);
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
          // If this alternative neighbor is sending to another node,
          // try to remove the receiving and create a pair.
          // Though we create a pair here, in adding tasks, we try to replace
          // the pair with the real pairing. The real pairing is created through
          // id calculation. Because it won't overlap, once it exists, we remove
          // the temporary pairing caused by node deletion.
          if (nOp[1] == 1) {
            List<int[]> nnOpDimList =
              nodeMap.get(nOp[0]).getNeighborOpList().get(dimID);
            int rmOpIndex = -1;
            // Check this neighbor's neighbor ops, remove receiving
            for (int i = 1; i < nnOpDimList.size(); i++) {
              if (nnOpDimList.get(i)[0] == neighborID) {
                rmOpIndex = i;
                break;
              }
            }
            nnOpDimList.remove(rmOpIndex);
            op[0] = neighborID;
            op[1] = 2;
            nOp[0] = nodeID;
            nOp[1] = 2;
          } else {
            // If it pairs with someone else
            op[0] = neighborID;
            op[1] = 1;
            // Add receiving
            nOpDimList.add(new int[] { nodeID, 0 });
          }
          // For neighbor ID < 0 are not recorded
          modifiedNodes.add(neighborID);
        }
      }
    } else {
      // Dead node isn't seen in the first op,
      // It is only in "receive" ops, so just remove them.
      List<Integer> remvOpIndexList = new LinkedList<Integer>();
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
    printLog(taskID + " fails.");
    // We decrease the number of running tasks.
    numRunningTasks.decrementAndGet();
    if (!isInitialized.get()) {
      deleteTask(activeTopo, taskID);
    } else {
      // Once the topology is initialized,
      // copy the current active topology to a sand box (maybe there is one, if
      // sandbox is not null), apply adding and see which neighbor nodes are
      // modified
      if (sandBox == null) {
        printLog("Copy active toopology to sandbox.");
        sandBox = copyTopology(activeTopo);
      }
      // There are failed tasks in the current topology
      isFailed.set(true);
      failCurrentNodeVersion(taskID);
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
      Set<String> modifiedNeighbors = new HashSet<>();
      for (Entry<Integer, HyperCubeNode> entry : sandBox.nodeMap.entrySet()) {
        modifiedNeighbors.add(entry.getValue().getTaskID());
      }
      modifiedNeighbors.remove(taskID);
      // Update the topology in sandbox after the failure
      // The modified neighbors returned by deleteTask are not used because
      // every task is notified
      deleteTask(sandBox, taskID);
      // Print and check the difference between two topologies
      printLog("Print active Hypercube.");
      printHyperCube(activeTopo);
      printLog("Print sandbox Hypercube.");
      printHyperCube(sandBox);
      boolean isFailureSent = false;
      for (String mTaskID : modifiedNeighbors) {
        // If neither don't contain this task ID
        if (!newTasks.contains(mTaskID) && !newTaskNeighbors.contains(mTaskID)
          && !failedTaskNeighbors.contains(mTaskID)) {
          printLog("Failed task neighbor: " + mTaskID);
          // Send failure message
          // The version is a number agreed in driver-task communication
          sendMsg(Utils.bldVersionedGCM(groupName, operName, Type.SourceDead,
            driverID, getNodeVersion(mTaskID), mTaskID,
            getNodeVersion(mTaskID), EmptyByteArr));
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

  private int failCurrentNodeVersion(String taskID) {
    AtomicInteger version = createNodeVersion(taskID);
    return version.incrementAndGet();
  }

  @Override
  public synchronized void processMsg(final GroupCommMessage msg) {
    // Invoked by TopologyMessageHandler when message arrives
    // We ignore TopologyChanges and UpdateTopology two messages
    // Assume tasks won't send these two types of messages.
    // Assume we only process SourceDead and SourceAdd two messages.
    // Be careful that when CommunicationGroupDriverImpl processes messages
    // it will check if all topologies get one message.
    // Because currently HyperCubeTopology is different from FlatTopology,
    // and it doesn't use all other types of messages,
    // there could cause problem in the code.
    // We assume there would only be AllReduce operator in the user code.
    if (msg.getSrcVersion() != getNodeVersion(msg.getSrcid())) {
      printLog("Processing " + msg.getType() + " from " + msg.getSrcid()
        + " with version " + msg.getSrcVersion() + ". But current version is "
        + getNodeVersion(msg.getSrcid()));
      return;
    }
    int iteration = getIterationInBytes(Utils.getData(msg));
    printLog("Processing " + msg.getType() + " from " + msg.getSrcid()
      + " with version " + msg.getSrcVersion() + " with iteration " + iteration);
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
    printLog("Process neighbor update.");
    if (numRunningTasks.get() < minInitialTasks) {
      // We don't process the neighbor update if we haven't got the
      // minimum number of running tasks.
      // Because the driver stop processing the message when the number of tasks
      // is less than the number of intial tasks, no message can arrive here.
      printLog("The current number of tasks is lower than"
        + " the number of running tasks required.");
      return;
    }
    // the method may be triggered when the old message from failed tasks is
    // received. Stop processing if no topology update is required.
    // if (failedTaskNeighbors.size() == 0 && newTaskNeighbors.size() == 0
    // && newTasks.size() == 0 && sandBox == null) {
    // System.out.println("No topology update required.");
    // return;
    // }
    // We collect reports from new task neighbors and failed task neighbors
    // For new tasks waiting for topology setup, they don't have task reports.
    boolean allContained = true;
    int minIteration = Integer.MAX_VALUE;
    int maxIteration = Integer.MIN_VALUE;
    Integer taskIteration = null;
    for (String taskID : failedTaskNeighbors) {
      taskIteration = taskReports.get(taskID);
      if (taskIteration != null) {
        if (minIteration > taskIteration.intValue()) {
          minIteration = taskIteration.intValue();
        }
        if (maxIteration < taskIteration.intValue()) {
          maxIteration = taskIteration.intValue();
        }
      } else {
        printLog("No report from current failed task neighbor: " + taskID);
        allContained = false;
        break;
      }
    }
    if (allContained) {
      // Check if all the task reports include all the new task neighbors
      for (String taskID : newTaskNeighbors) {
        taskIteration = taskReports.get(taskID);
        if (taskIteration != null) {
          if (minIteration > taskIteration.intValue()) {
            minIteration = taskIteration.intValue();
          }
          if (maxIteration < taskIteration.intValue()) {
            maxIteration = taskIteration.intValue();
          }
        } else {
          printLog("No report from current new task neighbor: " + taskID);
          allContained = false;
          break;
        }
      }
    }
    if (allContained) {
      if (minIteration > maxIteration) {
        // Probably all the tasks are dead (rare case),
        // then driver doesn't know which iteration all tasks are
        // we set the iteration to the current iteration.
        minIteration = iteration.get();
        maxIteration = iteration.get();
      }
      // If yes, calculate which iteration the new topology is for
      activeTopo = sandBox;
      // if (activeTopo == null) {
      //  System.out.println("null activeTopo 1");
      // }
      // Set the internal agreed iteration number for updating the topology.
      int baseIteration = minIteration;
      int newIteration = maxIteration + 1;
      // If newIteration is less than or equal to the current iteration value,
      // this means some topology has been sent out for this iteration. So you
      // cannot send another topology for the iteration. newIteration
      // has to be the one next to the current iteration number.
      if (iteration.get() >= newIteration) {
        newIteration = iteration.get() + 1;
      }
      iteration.set(newIteration);
      printLog("baseIteration: " + baseIteration + ", newIteration: "
        + newIteration + ", total number of nodes in new topology: "
        + activeTopo.nodeMap.size());
      // Because all tasks are blocked when there is failures.
      for (String taskID : failedTaskNeighbors) {
        // Send new topology
        HyperCubeNode node =
          activeTopo.nodeMap.get(activeTopo.taskMap.get(taskID));
        try {
          sendTopology(node, activeTopo, taskVersionMap, Type.UpdateTopology,
            baseIteration, newIteration, isFailed.get());
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
          sendTopology(node, activeTopo, taskVersionMap, Type.UpdateTopology,
            baseIteration, newIteration, isFailed.get());
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
          sendTopology(node, activeTopo, taskVersionMap, Type.TopologySetup,
            baseIteration, newIteration, false);
        } catch (Exception e) {
          e.printStackTrace();
          LOG.log(Level.WARNING, "", e);
          printLog("Fail to send the topology setup");
        }
      }
      // Clean related data structures
      taskReports.clear();
      failedTaskNeighbors.clear();
      newTaskNeighbors.clear();
      newTasks.clear();
      isFailed.set(false);
      sandBox = null;
    } else {
      printLog("Waiting for the arrival of more task reports.");
    }
  }

  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName)
      + " - ";
  }

  private void printHyperCube(TopoStruct topo) {
    // if (topo == null) {
    // topo = activeTopo;
    // }
    // HyperCubeNode node = null;
    // for (Entry<Integer, HyperCubeNode> entry : topo.nodeMap.entrySet()) {
    // node = entry.getValue();
    // List<List<int[]>> opList = entry.getValue().getNeighborOpList();
    // StringBuffer sb1 = new StringBuffer();
    // StringBuffer sb2 = new StringBuffer();
    // sb1.append(node.getNodeID() + ":|");
    // sb2.append(node.getNodeID() + ":|");
    // for (List<int[]> opDimList : opList) {
    // for (int[] op : opDimList) {
    // sb1.append(op[0] + " ");
    // sb2.append(op[1] + " ");
    // }
    // sb1.append("|");
    // sb2.append("|");
    // }
    // printLog(sb1.toString());
    // printLog(sb2.toString());
    // }
  }

  @Override
  public boolean isRunning(String taskId) {
    // This method is not invoked.
    printLog("Is topology running?");
    return false;
  }

  private void printLog(String log) {
    System.out.println(getQualifiedName() + log);
  }

  public static void main(String[] args) {
    HyperCubeTopology topo = new HyperCubeTopology(null, null, null, null, 128);
    for (int i = 0; i < 1; i++) {
      topo.newTask(null, "task-" + i);
    }
    topo.printHyperCube(null);
  }
}
