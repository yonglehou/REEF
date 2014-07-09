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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.driver.parameters.DriverIdentifier;
import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.group.operators.AllReduce;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.nggroup.api.CommGroupNetworkHandler;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.CommunicationGroupName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.DataCodec;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.OperatorName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.TaskVersion;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Identifier;

/**
 * AllReducer, which sends/ receives data with a list of neighbor nodes in the
 * hyper cube topology.
 * 
 */
public class AllReducer<T> implements AllReduce<T>,
  EventHandler<GroupCommMessage> {

  private static final Logger LOG = Logger
    .getLogger(AllReducer.class.getName());
  private static final byte[] EmptyByteArr = new byte[0];

  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final CommGroupNetworkHandler commGroupNetworkHandler;
  private final Codec<T> dataCodec;
  private final NetworkService<GroupCommMessage> netService;
  private final Sender sender;
  private final int version;

  private final ReduceFunction<T> reduceFunction;
  private final HyperCubeTopoClient topoClient;
  // private final CommunicationGroupClient commGroupClient;
  private final AtomicBoolean isInitialized = new AtomicBoolean(false);
  private final AtomicInteger iteration;

  // Data map for allreduce
  private final ConcurrentMap<Integer, ConcurrentMap<String, GroupCommMessage>> dataMap =
    new ConcurrentHashMap<>();
  // Data map for reduce scatter + allgather
  private final ConcurrentMap<Integer, ConcurrentMap<String, ConcurrentMap<Integer, GroupCommMessage>>> rsagDataMap =
    new ConcurrentHashMap<>();
  private final BlockingQueue<GroupCommMessage> dataQueue =
    new LinkedBlockingQueue<>();

  private final String selfID;
  private final String driverID;
  // Topology structure of this node
  private HyperCubeNode node;
  private Map<Integer, String> opTaskMap;
  private final AtomicBoolean isLastIteFailed = new AtomicBoolean(false);
  private int numFailedIterations = 0;

  // private final AtomicBoolean isNewTaskAdded = new AtomicBoolean(false);

  @Inject
  public AllReducer(
    @Parameter(CommunicationGroupName.class) final String groupName,
    @Parameter(OperatorName.class) final String operName,
    @Parameter(TaskConfigurationOptions.Identifier.class) final String selfId,
    @Parameter(DataCodec.class) final Codec<T> dataCodec,
    @Parameter(com.microsoft.reef.io.network.nggroup.impl.config.parameters.ReduceFunctionParam.class) final ReduceFunction<T> reduceFunction,
    @Parameter(DriverIdentifier.class) final String driverId,
    @Parameter(TaskVersion.class) final int version,
    final CommGroupNetworkHandler commGroupNetworkHandler,
    final NetworkService<GroupCommMessage> netService,
    final CommunicationGroupClient commGroupClient) {
    super();
    LOG.info(operName + " injection starts");
    this.version = version;
    this.iteration = new AtomicInteger(0);
    this.groupName = Utils.getClass(groupName);
    this.operName = Utils.getClass(operName);
    this.dataCodec = dataCodec;
    this.reduceFunction = reduceFunction;
    this.commGroupNetworkHandler = commGroupNetworkHandler;
    this.netService = netService;
    this.sender = new Sender(this.netService);
    // Here we HyperCubeTopoClient to do sending and receiving at the client
    // side with following HyperCubeTopology, the topology controller at the
    // driver side.
    this.topoClient =
      new HyperCubeTopoClient(this.groupName, this.operName, selfId, driverId,
        sender, version);
    this.commGroupNetworkHandler.register(this.operName, this);
    // this.commGroupClient = commGroupClient;
    selfID = selfId;
    driverID = driverId;
    LOG.info(operName + " injection ends.");
  }

  @Override
  public int getVersion() {
    // Use a number agreed by client and driver in hypercube topology
    // and allreduce related code. But because this method only invoked by
    // getTopologyChanges and UpdateTopology two methods which are not used by
    // allreduce. No affect.
    return version;
  }

  @Override
  public void initialize() {
    // Due to the control on CommunicationGroupClientImpl initialization
    // This code won't be invoked repeatedly.
    System.out.println("Initialize AllReducer.");
    topoClient.waitForNewNodeTopology();
    NodeTopology nodeTopo = topoClient.getNewestNodeTopology();
    // This should not be null
    node = nodeTopo.node;
    opTaskMap = nodeTopo.opTaskMap;
    iteration.set(nodeTopo.newIteration);
  }

  @Override
  public Class<? extends Name<String>> getOperName() {
    return operName;
  }

  @Override
  public Class<? extends Name<String>> getGroupName() {
    return groupName;
  }

  @Override
  public void onNext(final GroupCommMessage msg) {
    // This method can be invoked simultaneously with apply method
    // If the msg type is TopologyChange or TopologyUpdated
    // they cannot arrive here.
    // There is no version checking when sending message between tasks
    // If the message types are not the two mentioned above, they are processed
    // here directly.
    if (msg.getType() == Type.AllReduce) {
      // Messages are put in the queue
      // and wait to be processed in "apply"
      System.out.println(getQualifiedName() + " Get AllReduce message from "
        + msg.getSrcid() + " with version " + msg.getSrcVersion());
      dataQueue.add(msg);
    } else {
      if (msg.getSrcVersion() != version) {
        System.out.println(getQualifiedName()
          + " Current task side node version is " + version
          + ", which is different from the driver side node version "
          + msg.getSrcVersion());
        return;
      }
      // Only driver sends these messages
      if (msg.getType() == Type.SourceDead) {
        dataQueue.add(msg);
      } else if (msg.getType() == Type.SourceAdd) {
        dataQueue.add(msg);
      } else {
        // Topology related messages are processed by topoClient
        topoClient.handle(msg);
      }
    }
  }

  @Override
  public ReduceFunction<T> getReduceFunction() {
    return reduceFunction;
  }

  @Override
  public T apply(T aElement) throws InterruptedException, NetworkException {
    if (isInitialized.compareAndSet(false, true)) {
      // This goes to the topology client initialization
      // commGroupClient.initialize();
      // Because the call above just loop through all the operators and
      // initialize each of them
      // Here we just initialize this operator only
      initialize();
    }
    int ite = iteration.get();
    System.out.println("Current iteration: " + ite);
    boolean isFailed = false;
    // Update topology for the iteration
    updateNodeTopology(ite);
    T reducedValue = aElement;
    List<byte[]> valBytes = new ArrayList<byte[]>();
    List<T> vals = new ArrayList<T>();
    // Record the last op performed in dim
   int[] lastPerformedOpIndex = new int[] {-1};
    int exitCode = -1;
    for (int i = 0; i < node.getNeighborOpList().size(); i++) {
      exitCode =
        applyInDim(reducedValue, ite, selfID, valBytes, node
          .getNeighborOpList().get(i), opTaskMap, lastPerformedOpIndex, version);
      if (exitCode == 0) {
        vals.add(reducedValue);
        for (byte[] data : valBytes) {
          vals.add(dataCodec.decode(data));
        }
        System.out.println("Allreduce - number of data received: "
          + (vals.size() - 1));
        reducedValue = reduceFunction.apply(vals);
        // Reset
        valBytes.clear();
        vals.clear();
        lastPerformedOpIndex[0] = -1;
      } else if (exitCode == 1) {
        // Ask driver, send source failure message
        // The version number is a number agreed between driver and task
        sendMsg(Type.SourceDead, selfID, version, driverID, version,
          getIterationInBytes(ite));
        // Get the new topology
        System.out
          .println("Waiting for the new topology after getting SourceDead.");
        waitForNewNodeTopology(topoClient, ite);
        // Fail the current iteration
        // This should match with topoClient.isFailed();
        isFailed = true;
        break;
      } else if (exitCode == 2) {
        // Ask driver, send source add message
        sendMsg(Type.SourceAdd, selfID, version, driverID, version,
          getIterationInBytes(ite));
        // Get the new topology
        System.out
          .println("Waiting for the new topology after getting SourceAdd");
        waitForNewNodeTopology(topoClient, ite);
        // Check if there is failure in the current topology
        isFailed = topoClient.getNewestNodeTopology().isFailed;
        if (isFailed) {
          break;
        }
        // Restart this dimension
        // continue with the ops not performed
        i--;
      }
    }
    if (isFailed) {
      reducedValue = null;
      isLastIteFailed.set(true);
      numFailedIterations =
        ite - topoClient.getNewestNodeTopology().baseIteration + 1;
      // Remove received byte data between this iteration and the new iteration
      // Because in failure state, every task is stopped and updated with new
      // topology, tasks no longer send messages of failed iterations.
      StringBuffer sb = new StringBuffer();
      for (int i = ite; i < topoClient.getNewestNodeTopology().newIteration; i++) {
        sb.append(i + " ");
        dataMap.remove(i);
      }
      System.out.println("Remove data from iteration " + sb);
      // If failure happens, move to the new iteration assigned by the driver
      iteration.set(topoClient.getNewestNodeTopology().newIteration);
    } else {
      isLastIteFailed.set(false);
      numFailedIterations = 0;
      dataMap.remove(ite);
      System.out.println("Remove data from iteration " + ite);
      iteration.incrementAndGet();
    }
    return reducedValue;
  }

  private void updateNodeTopology(int iteration) {
    // If there is no new topology, keep the old one.
    NodeTopology nodeTopo = topoClient.getNodeTopology(iteration);
    if (nodeTopo != null) {
      node = nodeTopo.node;
      opTaskMap = nodeTopo.opTaskMap;
      // If update successful, remove the topologies whose iteration number is
      // lower than the current one
      topoClient.removeOldNodeTopologies(iteration);
    }
  }

  private void waitForNewNodeTopology(HyperCubeTopoClient topoClient,
    int currentIteration) {
    // Get the new topology
    topoClient.waitForNewNodeTopology();
    System.out.println("The current iteration: " + currentIteration
      + ", the base iteration of topology got from the driver: "
      + topoClient.getNewestNodeTopology().baseIteration
      + ", the new iteration of topology got from the driver: "
      + topoClient.getNewestNodeTopology().newIteration + ", is failed? "
      + topoClient.getNewestNodeTopology().isFailed);
  }

  private int applyInDim(T reducedValue, int iteration, String selfID,
    List<byte[]> valBytes, List<int[]> opDimList,
    Map<Integer, String> opTaskMap, int[] lastPerformedOpIndex, int version)
    throws NetworkException, InterruptedException {
    String taskID = null;
    int[] op = null;
    byte[] bytes = null;
    int lastIndex = lastPerformedOpIndex[0];
    // If the last Performed OP index is less than 0 (negative, means nothing
    // performed), i starts with 0. otherwise, it starts with the last performed
    // op index.
    if (lastIndex >= 0) {
      System.out.println("Resume Allreduce on op with index " + lastIndex);
    }
    int i = lastIndex < 0 ? 0 : lastIndex;
    for (; i < opDimList.size(); i++) {
      op = opDimList.get(i);
      taskID = opTaskMap.get(op[0]);
      System.out.println("Apply, Task ID: " + taskID + ", op: [" + op[0] + ","
        + op[1] + "]");
      if (i != lastIndex) {
        if (op[1] == 1 || op[1] == 2) {
          // Send message to other tasks
          // We set version to the iteration.
          System.out.println("Allreduce - send one data.");
          sendMsg(Type.AllReduce, selfID, iteration, taskID, 0,
            dataCodec.encode(reducedValue));
        }
      }
      // For op 0, 1, 2, all need "receiving"
      if (op[1] != -1) {
        // Receive data
        GroupCommMessage msg = null;
        do {
          // Allreduce uses commType 0
          msg = receiveMsg(iteration, taskID, 0);
          if (msg.getType() == Type.AllReduce) {
            bytes = Utils.getData(msg);
            // If op is 0 or 2, the data is needed
            if (op[1] != 1) {
              valBytes.add(bytes);
            }
          } else if (msg.getType() == Type.SourceDead) {
            // Source Dead message from driver
            // ignore the message mistakenly put here
            if (msg.getSrcVersion() == version) {
              lastPerformedOpIndex[0] = i;
              return 1;
            } else {
              msg = null;
            }
          } else if (msg.getType() == Type.SourceAdd) {
            // Source Add message from the driver
            // ignore the message mistakenly put here
            if (msg.getSrcVersion() == version) {
              lastPerformedOpIndex[0] = i;
              return 2;
            } else {
              msg = null;
            }
          }
        } while (msg == null);
      }
      // If there is no failure in receiving,
      // we continue the original operations.
      // For receiving only operation, send an ack message.
      if (op[1] == 0) {
        // send an ack message to the sender task
        // We set version to iteration.
        sendMsg(Type.AllReduce, selfID, iteration, taskID, 0, EmptyByteArr);
      }
    }
    lastPerformedOpIndex[0] = opDimList.size();
    // Success
    return 0;
  }

  private void sendMsg(Type msgType, String selfID, int srcVersion,
    String taskID, int tgtVersion, byte[]... bytes) {
    try {
      sender.send(Utils.bldVersionedGCM(groupName, operName, msgType, selfID,
        srcVersion, taskID, tgtVersion, bytes));
    } catch (NetworkException e) {
      e.printStackTrace();
    }
  }

  private byte[] getIterationInBytes(int iteration) {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(bout);
    try {
      dout.writeInt(iteration);
      dout.flush();
      dout.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return bout.toByteArray();
  }

  private GroupCommMessage receiveMsg(int iteration, String neighborID,
    int commType) throws InterruptedException {
    // Comm type:
    // 0: allreduce on one message
    // 1: reducescatter on chunk messages
    // -1: allgather on chunk messages
    GroupCommMessage msg = null;
    do {
      msg = getMsgFromMap(iteration, neighborID, commType);
      if (msg != null) {
        return msg;
      }
      msg = dataQueue.take();
      if (msg.getType() == Type.SourceDead) {
        return msg;
      } else if (msg.getType() == Type.SourceAdd) {
        return msg;
      } else if (msg.getType() == Type.AllReduce) {
        // Source version is used as iteration
        // Target version is used as communication type
        if (msg.getSrcid().equals(neighborID)
          && msg.getSrcVersion() == iteration && msg.getVersion() == commType) {
          return msg;
        } else {
          addMsgToMap(msg);
          msg = null;
        }
      }
    } while (msg == null);
    return msg;
  }

  private GroupCommMessage getMsgFromMap(int iteration, String taskID,
    int commType) {
    if (commType == 0) {
      ConcurrentMap<String, GroupCommMessage> iteMap = dataMap.get(iteration);
      if (iteMap == null) {
        iteMap = new ConcurrentHashMap<>();
        ConcurrentMap<String, GroupCommMessage> oldIteMap =
          dataMap.putIfAbsent(iteration, iteMap);
        if (oldIteMap != null) {
          iteMap = oldIteMap;
        }
      }
      return iteMap.remove(taskID);
    } else {
      Map<Integer, GroupCommMessage> msgMap = getRsAgMsgMap(iteration, taskID);
      return msgMap.remove(commType);
    }
  }

  private void addMsgToMap(GroupCommMessage msg) {
    if (msg.getVersion() == 0) {
      ConcurrentMap<String, GroupCommMessage> iteMap =
        dataMap.get(msg.getSrcVersion());
      if (iteMap == null) {
        iteMap = new ConcurrentHashMap<>();
        ConcurrentMap<String, GroupCommMessage> oldIteMap =
          dataMap.putIfAbsent(msg.getSrcVersion(), iteMap);
        if (oldIteMap != null) {
          iteMap = oldIteMap;
        }
      }
      iteMap.put(msg.getSrcid(), msg);
    } else {
      Map<Integer, GroupCommMessage> msgMap =
        getRsAgMsgMap(msg.getSrcVersion(), msg.getSrcid());
      msgMap.put(msg.getVersion(), msg);
    }
  }

  private ConcurrentMap<Integer, GroupCommMessage> getRsAgMsgMap(int iteration,
    String taskID) {
    ConcurrentMap<String, ConcurrentMap<Integer, GroupCommMessage>> iteMap =
      rsagDataMap.get(iteration);
    if (iteMap == null) {
      iteMap = new ConcurrentHashMap<>();
      ConcurrentMap<String, ConcurrentMap<Integer, GroupCommMessage>> oldIteMap =
        rsagDataMap.putIfAbsent(iteration, iteMap);
      if (oldIteMap != null) {
        iteMap = oldIteMap;
      }
    }
    ConcurrentMap<Integer, GroupCommMessage> msgMap = iteMap.get(taskID);
    if (msgMap == null) {
      msgMap = new ConcurrentHashMap<>();
      ConcurrentMap<Integer, GroupCommMessage> oldMsgMap =
        iteMap.putIfAbsent(taskID, msgMap);
      if (oldMsgMap != null) {
        msgMap = oldMsgMap;
      }
    }
    return msgMap;
  }

  public List<T> apply(List<T> elements) throws InterruptedException,
    NetworkException {
    // See if we can apply large message data
    // We need to change the interface to make chunking automatic.
    // Assume the elements are ordered.
    if (isInitialized.compareAndSet(false, true)) {
      initialize();
    }
    int ite = iteration.get();
    System.out.println("Current iteration: " + ite);
    boolean isFailed = false;
    updateNodeTopology(ite);
    // Data structure for allreduce
    Map<Integer, T> reducedValMap = new TreeMap<>();
    for (int i = 0; i < elements.size(); i++) {
      reducedValMap.put(i, elements.get(i));
    }
    System.out.println("Num of chunks of ReduceScatter + Allgather (start): "
      + reducedValMap.size());
    // Data structure to collect allreduce results
    List<T> reducedVals = null;
    // Do reduce scatter
    isFailed =
      reduceScatter(reducedValMap, ite, selfID, node.getNodeID(),
        node.getNeighborOpList(), opTaskMap, dataCodec, reduceFunction,
        driverID, topoClient, version);
    // Do allgather
    if (!isFailed) {
      isFailed =
        allGather(reducedValMap, ite, selfID, node.getNodeID(),
          node.getNeighborOpList(), opTaskMap, dataCodec, reduceFunction,
          driverID, topoClient, version);
    }
    if (isFailed) {
      reducedVals = null;
      isLastIteFailed.set(true);
      numFailedIterations =
        ite - topoClient.getNewestNodeTopology().baseIteration + 1;
      // Remove received byte data between this iteration and the new iteration
      StringBuffer sb = new StringBuffer();
      for (int i = ite; i < topoClient.getNewestNodeTopology().newIteration; i++) {
        sb.append(i + " ");
        rsagDataMap.remove(i);
      }
      System.out.println("Remove data from iteration " + sb);
      // If failure happens, move to the new iteration assigned by the driver
      iteration.set(topoClient.getNewestNodeTopology().newIteration);
    } else {
      // Remove the reduced vals in the map and put to the list.
      // Keep the order.
      reducedVals = new ArrayList<>(reducedValMap.size());
      System.out.println("Num of chunks of ReduceScatter + Allgather (end): "
        + reducedValMap.size());
      for (Entry<Integer, T> entry : reducedValMap.entrySet()) {
        reducedVals.add(entry.getKey().intValue(), entry.getValue());
      }
      reducedValMap.clear();
      isLastIteFailed.set(false);
      numFailedIterations = 0;
      rsagDataMap.remove(ite);
      System.out.println("Remove data from iteration " + ite);
      iteration.incrementAndGet();
    }
    return reducedVals;
  }

  private boolean reduceScatter(Map<Integer, T> reducedValMap, int iteration,
    String selfID, int nodeID, List<List<int[]>> opList,
    Map<Integer, String> opTaskMap, Codec<T> codec,
    ReduceFunction<T> reduceFunc, String driverID,
    HyperCubeTopoClient topoClient, int version) throws InterruptedException {
    // Record operations performed on this dimension
    int[] lastPerformedOpIndex = new int[] { -1 };
    // Chunk ID <-> byte messages from other nodes
    Map<Integer, List<byte[]>> valByteMap = new HashMap<>();
    // Used in reduce function
    List<T> vals = new ArrayList<T>();
    boolean isFailed = false;
    int moduloBase = 2;
    int exitCode = -1;
    // Do reduce scatter
    for (int i = 0; i < opList.size(); i++) {
      // For each dimension, follow the topology and send the data
      exitCode =
        reduceScatterInDim(reducedValMap, iteration, selfID, nodeID,
          valByteMap, opList.get(i), opTaskMap, moduloBase, codec,
          lastPerformedOpIndex, version);
      if (exitCode == 0) {
        // Apply on each chunk ID
        System.out.println("Num of chunks received: " + valByteMap.size());
        for (Entry<Integer, List<byte[]>> entry : valByteMap.entrySet()) {
          int chunkID = entry.getKey().intValue();
          List<byte[]> valBytes = entry.getValue();
          T reducedVal = reducedValMap.get(chunkID);
          if (reducedVal != null) {
            vals.add(reducedVal);
          }
          // If this value is not owned, reduce on received values.
          for (byte[] data : valBytes) {
            vals.add(codec.decode(data));
          }
          if (vals.size() > 1) {
            reducedValMap.put(chunkID, reduceFunc.apply(vals));
          } else {
            // If only one received value and no owned value
            reducedValMap.put(chunkID, vals.get(0));
          }
          vals.clear();
        }
        // Reset
        valByteMap.clear();
        lastPerformedOpIndex[0] = -1;
        // Modulo base only gets changed
        // when success to perform operations.
        moduloBase *= 2;
      } else if (exitCode == 1) {
        // Ask driver, send source failure message
        sendMsg(Type.SourceDead, selfID, version, driverID, version,
          getIterationInBytes(iteration));
        // Get the new topology
        System.out
          .println("Waiting for the new topology after getting SourceDead.");
        waitForNewNodeTopology(topoClient, iteration);
        // Fail the current iteration
        // This should match with topoClient.isFailed();
        isFailed = true;
        break;
      } else if (exitCode == 2) {
        // Ask driver, send source add message
        // The version number is always 0 when sending to the driver
        sendMsg(Type.SourceAdd, selfID, version, driverID, version,
          getIterationInBytes(iteration));
        // Get the new topology
        System.out
          .println("Waiting for the new topology after getting SourceAdd");
        waitForNewNodeTopology(topoClient, iteration);
        // Check if there is failure in the current topology
        isFailed = topoClient.getNewestNodeTopology().isFailed;
        if (isFailed) {
          break;
        }
        // Restart this dimension if it is not failed.
        // continue with the ops not performed
        i--;
      }
    }
    return isFailed;
  }

  private int reduceScatterInDim(Map<Integer, T> reducedValMap, int iteration,
    String selfID, int nodeID, Map<Integer, List<byte[]>> valByteMap,
    List<int[]> opDimList, Map<Integer, String> opTaskMap, int moduloBase,
    Codec<T> codec, int[] lastPerformedOpIndex, int version)
    throws InterruptedException {
    String taskID = null;
    int[] op = null;
    int lastIndex = lastPerformedOpIndex[0];
    // If the last Performed OP index is less than 0 (negative, means nothing
    // performed), i starts with 0. otherwise, it starts with the last performed
    // op index.
    if (lastIndex >= 0) {
      System.out.println("Resume ReduceScatter on op with index " + lastIndex);
    }
    int i = lastIndex < 0 ? 0 : lastIndex;
    for (; i < opDimList.size(); i++) {
      op = opDimList.get(i);
      taskID = opTaskMap.get(op[0]);
      System.out.println("ReduceScatter, Task ID: " + taskID + ", op: ["
        + op[0] + "," + op[1] + "]");
      if (i != lastIndex) {
        // If op is not last op, do as normal
        // otherwise, if the code exits because of ResourceDead and ResourceAdd
        // we record the index and resume receiving.
        if (op[1] == 1 || op[1] == 2) {
          List<Integer> chunkIDs = new ArrayList<>();
          List<byte[]> byteList = new ArrayList<>();
          // Send chunks not belonged to this node
          for (Entry<Integer, T> entry : reducedValMap.entrySet()) {
            int chunkID = entry.getKey().intValue();
            if ((nodeID % moduloBase != chunkID % moduloBase)
              && (op[0] % moduloBase == chunkID % moduloBase)) {
              chunkIDs.add(chunkID);
              byteList.add(codec.encode(entry.getValue()));
            }
          }
          // Remove chunks sent out
          for (int chunkID : chunkIDs) {
            reducedValMap.remove(chunkID);
          }
          System.out.println("ReduceScatter - Num of chunks sent: "
            + chunkIDs.size());
          byteList.add(0, getChunkIDsToBytes(chunkIDs));
          sendMsg(Type.AllReduce, selfID, iteration, taskID, 1,
            byteList.toArray(new byte[byteList.size()][]));
        }
      }
      // For op 0, 1, 2, all need "receiving"
      if (op[1] != -1) {
        GroupCommMessage msg = null;
        do {
          // Reduce Scatter in Allreduce uses commType 1
          msg = receiveMsg(iteration, taskID, 1);
          if (msg.getType() == Type.AllReduce) {
            // If op is 0 or 2, the data is needed
            if (op[1] != 1) {
              getRsAgDataFromMsg(valByteMap, msg);
            }
          } else if (msg.getType() == Type.SourceDead) {
            // Source Dead message from driver
            // ignore the message mistakenly put here
            if (msg.getSrcVersion() == version) {
              lastPerformedOpIndex[0] = i;
              return 1;
            } else {
              msg = null;
            }
          } else if (msg.getType() == Type.SourceAdd) {
            // Source Add message from the driver
            // ignore the message mistakenly put here
            if (msg.getSrcVersion() == version) {
              lastPerformedOpIndex[0] = i;
              return 2;
            } else {
              msg = null;
            }
          }
        } while (msg == null);
      }
      // If there is no failure in receiving,
      // we continue the original operations.
      // For receiving only operation, send an ack message.
      if (op[1] == 0) {
        // send an ack message to the sender task
        // The source version is the iteration
        sendMsg(Type.AllReduce, selfID, iteration, taskID, 1, EmptyByteArr);
      }
    }
    // All ops are performed
    lastPerformedOpIndex[0] = opDimList.size();
    // Success
    return 0;
  }

  private byte[] getChunkIDsToBytes(List<Integer> chunkIDs) {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(bout);
    byte[] bytes = null;
    try {
      dout.writeInt(chunkIDs.size());
      for (int i = 0; i < chunkIDs.size(); i++) {
        dout.writeInt(chunkIDs.get(i));
      }
      dout.flush();
      dout.close();
      bytes = bout.toByteArray();
    } catch (IOException e) {
      bytes = null;
      e.printStackTrace();
    }
    return bytes;
  }

  private List<Integer> getChunkIDsFromBytes(byte[] bytes) {
    ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
    DataInputStream din = new DataInputStream(bin);
    int idCount = 0;
    List<Integer> chunkIDs = null;
    try {
      idCount = din.readInt();
      chunkIDs = new ArrayList<>(idCount);
      for (int i = 0; i < idCount; i++) {
        chunkIDs.add(din.readInt());
      }
      din.close();
    } catch (IOException e) {
      chunkIDs = null;
      e.printStackTrace();
    }
    return chunkIDs;
  }

  private void getRsAgDataFromMsg(
    Map<Integer, List<byte[]>> valByteMap, GroupCommMessage msg) {
    List<Integer> chunkIDs =
      getChunkIDsFromBytes(msg.getMsgs(0).getData().toByteArray());
    int chunkID = -1;
    for (int i = 0; i < chunkIDs.size(); i++) {
      chunkID = chunkIDs.get(i);
      List<byte[]> byteList = valByteMap.get(chunkID);
      if (byteList == null) {
        byteList = new ArrayList<>();
        valByteMap.put(chunkID, byteList);
      }
      byteList.add(msg.getMsgs(i + 1).getData().toByteArray());
    }
  }

  private boolean allGather(Map<Integer, T> reducedValMap, int iteration,
    String selfID, int nodeID, List<List<int[]>> opList,
    Map<Integer, String> opTaskMap, Codec<T> codec,
    ReduceFunction<T> reduceFunc, String driverID,
    HyperCubeTopoClient topoClient, int version) throws InterruptedException {
    int[] lastPerformedOpIndex = new int[] { -1 };
    // Chunk ID <-> byte messages from other nodes
    Map<Integer, List<byte[]>> valByteMap = new HashMap<>();
    // Used in reduce function
    List<T> vals = new ArrayList<T>();
    boolean isFailed = false;
    int exitCode = -1;
    // Do allgather
    for (int i = 0; i < opList.size(); i++) {
      // For each dimension, follow the topology and send the data
      exitCode =
        allgatherInDim(reducedValMap, iteration, selfID, nodeID, valByteMap,
          opList.get(i), opTaskMap, codec, lastPerformedOpIndex, version);
      if (exitCode == 0) {
        // Apply each chunk received
        System.out.println("Allgather - Num of chunks received: "
          + valByteMap.size());
        for (Entry<Integer, List<byte[]>> entry : valByteMap.entrySet()) {
          int chunkID = entry.getKey().intValue();
          List<byte[]> valBytes = entry.getValue();
          T reducedVal = reducedValMap.get(chunkID);
          if (reducedVal != null) {
            vals.add(reducedVal);
          }
          // If this value is not owned, reduce on received values.
          for (byte[] data : valBytes) {
            vals.add(codec.decode(data));
          }
          // Reduce scatter may not reduce all chunks when there is dead zones
          // So we continue do reduce in allgather.
          // To avoid dead zone, we may need to rebalance the topology
          // when failure happens.
          if (vals.size() > 1) {
            reducedValMap.put(chunkID, reduceFunc.apply(vals));
          } else {
            reducedValMap.put(chunkID, vals.get(0));
          }
          vals.clear();
        }
        // Reset
        valByteMap.clear();
        lastPerformedOpIndex[0] = -1;
      } else if (exitCode == 1) {
        // Ask driver, send source failure message
        sendMsg(Type.SourceDead, selfID, version, driverID, version,
          getIterationInBytes(iteration));
        // Get the new topology
        System.out
          .println("Waiting for the new topology after getting SourceDead.");
        waitForNewNodeTopology(topoClient, iteration);
        // Fail the current iteration
        // This should match with topoClient.isFailed();
        isFailed = true;
        break;
      } else if (exitCode == 2) {
        // Ask driver, send source add message
        // The version number is always 0 when sending to the driver
        sendMsg(Type.SourceAdd, selfID, version, driverID, version,
          getIterationInBytes(iteration));
        // Get the new topology
        System.out
          .println("Waiting for the new topology after getting SourceAdd");
        waitForNewNodeTopology(topoClient, iteration);
        // Check if there is failure in the current topology
        isFailed = topoClient.getNewestNodeTopology().isFailed;
        if (isFailed) {
          break;
        }
        // Restart this dimension if it is not failed.
        // continue with the ops not performed
        i--;
      }
    }
    return isFailed;
  }

  private int allgatherInDim(Map<Integer, T> reducedValMap, int iteration,
    String selfID, int nodeID, Map<Integer, List<byte[]>> valByteMap,
    List<int[]> opDimList, Map<Integer, String> opTaskMap, Codec<T> codec,
    int[] lastPerformedOpIndex, int version) throws InterruptedException {
    String taskID = null;
    int[] op = null;
    int lastIndex = lastPerformedOpIndex[0];
    // If the last Performed OP index is less than 0 (negative, means nothing
    // performed), i starts with 0. otherwise, it starts with the last performed
    // op index.
    if (lastIndex >= 0) {
      System.out.println("Resume Allgather on op with index " + lastIndex);
    }
    int i = lastIndex < 0 ? 0 : lastIndex;
    for (; i < opDimList.size(); i++) {
      op = opDimList.get(i);
      taskID = opTaskMap.get(op[0]);
      System.out.println("Allgather, Task ID: " + taskID + ", op: [" + op[0]
        + "," + op[1] + "]");
      if (i != lastIndex) {
        if (op[1] == 1 || op[1] == 2) {
          List<Integer> chunkIDs = new ArrayList<>();
          List<byte[]> byteList = new ArrayList<>();
          for (Entry<Integer, T> entry : reducedValMap.entrySet()) {
            int chunkID = entry.getKey().intValue();
            chunkIDs.add(chunkID);
            byteList.add(codec.encode(entry.getValue()));
          }
          System.out.println("Allgather - Num of chunks sent: "
            + chunkIDs.size());
          byteList.add(0, getChunkIDsToBytes(chunkIDs));
          sendMsg(Type.AllReduce, selfID, iteration, taskID, -1,
            byteList.toArray(new byte[byteList.size()][]));
        }
      }
      // For op 0, 1, 2, all need "receiving"
      if (op[1] != -1) {
        GroupCommMessage msg = null;
        do {
          // Allgather in Allreduce uses commType -1
          msg = receiveMsg(iteration, taskID, -1);
          if (msg.getType() == Type.AllReduce) {
            // If op is 0 or 2, the data is needed
            if (op[1] != 1) {
              getRsAgDataFromMsg(valByteMap, msg);
            }
          } else if (msg.getType() == Type.SourceDead) {
            // Source Dead message from driver
            // ignore the message mistakenly put here
            if (msg.getSrcVersion() == version) {
              lastPerformedOpIndex[0] = i;
              return 1;
            } else {
              msg = null;
            }
          } else if (msg.getType() == Type.SourceAdd) {
            // Source Add message from the driver
            // ignore the message mistakenly put here
            if (msg.getSrcVersion() == version) {
              lastPerformedOpIndex[0] = i;
              return 2;
            } else {
              msg = null;
            }
          }
        } while (msg == null);
      }
      // If there is no failure in receiving,
      // we continue the original operations.
      // For receiving only operation, send an ack message.
      if (op[1] == 0) {
        // send an ack message to the sender task
        // The source version is the iteration
        // The target version is the commType
        sendMsg(Type.AllReduce, selfID, iteration, taskID, -1, EmptyByteArr);
      }
    }
    lastPerformedOpIndex[0] = opDimList.size();
    // Success
    return 0;
  }

  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName) + ":"
      + selfID + " - ";
  }

  /**
   * Check if there is failure in the last "apply".
   * 
   * @return If the last iteration is failed.
   */
  public boolean isLastIterationFailed() {
    return this.isLastIteFailed.get();
  }

  /**
   * Get the number of failed iterations. 0 means no failure.
   * 
   * @return
   */
  public int getNumFailedIterations() {
    return this.numFailedIterations;
  }

  @Override
  public T apply(T element, List<? extends Identifier> order)
    throws InterruptedException, NetworkException {
    // Disable this method as what ReduceReceiver does.
    throw new UnsupportedOperationException();
  }
}
