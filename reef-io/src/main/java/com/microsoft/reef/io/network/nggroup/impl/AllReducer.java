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
import java.util.List;
import java.util.Map;
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

  private final ConcurrentMap<Integer, ConcurrentMap<String, GroupCommMessage>> dataMap =
    new ConcurrentHashMap<>();
  private final BlockingQueue<GroupCommMessage> dataQueue =
    new LinkedBlockingQueue<>();

  private final String selfID;
  private final String driverID;
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
    if (msg.getType() == Type.AllReduce) {
      // Messages are put in the queue
      // and wait to be processed in "apply"
      dataQueue.add(msg);
    } else {
      if (msg.getSrcVersion() != version) {
        System.out.println(getQualifiedName()
          + "Current task side node version is " + version
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
    // Update topology when either condition comes true
    // if (isLastIteFailed.get() || isNewTaskAdded.get()) {
    // Actually if the last iteration is failed, the iteration will be set to
    // the new iteration, then the control flow should be able to enter this
    // block
    // if (ite == topoClient.getNewIteration()) {
    // updateTopology();
    // if (isNewTaskAdded.get()) {
    // isNewTaskAdded.set(false);
    // }
    // }
    // }
    updateTopology(ite);
    T reducedValue = aElement;
    List<byte[]> valBytes = new ArrayList<byte[]>();
    List<T> vals = new ArrayList<T>();
    Map<Integer, Integer> opPerformed = new HashMap<>();
    int exitCode = -1;
    for (int i = 0; i < node.getNeighborOpList().size(); i++) {
      exitCode =
        applyInDim(reducedValue, ite, selfID, driverID, valBytes, node
          .getNeighborOpList().get(i), opTaskMap, opPerformed, version);
      if (exitCode == 0) {
        vals.add(reducedValue);
        for (byte[] data : valBytes) {
          vals.add(dataCodec.decode(data));
        }
        reducedValue = reduceFunction.apply(vals);
        valBytes.clear();
        vals.clear();
        opPerformed.clear();
      } else if (exitCode == 1) {
        // Ask driver, send source failure message
        // The version number is a number agreed between driver and task
        sendMsg(Type.SourceDead, selfID, driverID, version,
          getIterationInBytes(ite));
        // Get the new topology
        System.out
          .println("Waiting for the new topology after getting SourceDead.");
        topoClient.waitForNewNodeTopology();
        System.out.println("The current iteration: " + ite
          + ", the base iteration of topology got from the driver: "
          + topoClient.getNewestNodeTopology().baseIteration
          + ", the new iteration of topology got from the driver: "
          + topoClient.getNewestNodeTopology().newIteration + ", is failed? "
          + topoClient.getNewestNodeTopology().isFailed);
        // Fail the current iteration
        // This should match with topoClient.isFailed();
        isFailed = true;
        break;
      } else if (exitCode == 3) {
        // Ask driver, send source add message
        // The version number is always 0 when sending to the driver
        sendMsg(Type.SourceAdd, selfID, driverID, version,
          getIterationInBytes(ite));
        // Get the new topology
        System.out
          .println("Waiting for the new topology after getting SourceAdd");
        topoClient.waitForNewNodeTopology();
        System.out.println("The current iteration: " + ite
          + ", the base iteration of topology got from the driver: "
          + topoClient.getNewestNodeTopology().baseIteration
          + ", the new iteration of topology got from the driver: "
          + topoClient.getNewestNodeTopology().newIteration + ", is failed? "
          + topoClient.getNewestNodeTopology().isFailed);
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
      // Remove received byte data between this iteration and the new iteration
      for (int i = ite; i < topoClient.getNewestNodeTopology().newIteration; i++) {
        dataMap.remove(i);
      }
      isLastIteFailed.set(true);
      numFailedIterations =
        iteration.get() - topoClient.getNewestNodeTopology().baseIteration + 1;
      // If failure happens, move to the new iteration assigned by the driver
      iteration.set(topoClient.getNewestNodeTopology().newIteration);
    } else {
      dataMap.remove(ite);
      isLastIteFailed.set(false);
      numFailedIterations = 0;
      iteration.incrementAndGet();
    }
    return reducedValue;
  }

  private void updateTopology(int iteration) {
    // If there is no new topology, keep the old one.
    NodeTopology nodeTopo = topoClient.getNodeTopology(iteration);
    if (nodeTopo != null) {
      node = nodeTopo.node;
      opTaskMap = nodeTopo.opTaskMap;
    }
  }

  private int applyInDim(T reducedValue, int iteration, String selfID,
    String driverID, List<byte[]> valBytes, List<int[]> opDimList,
    Map<Integer, String> opTaskMap, Map<Integer, Integer> opPerformed,
    int version) throws NetworkException, InterruptedException {
    String taskID = null;
    int[] op = null;
    byte[] bytes = null;
    for (int i = 0; i < opDimList.size(); i++) {
      op = opDimList.get(i);
      if (!opPerformed.containsKey(op[0])) {
        // If op is not performed, continue
        taskID = opTaskMap.get(op[0]);
        System.out.println("Apply, Task ID: " + taskID + ", op ID: " + op[1]);
        if (op[1] == 1 || op[1] == 2) {
          // Send message to other tasks
          // We set version to the iteration.
          sendMsg(Type.AllReduce, selfID, taskID, iteration,
            dataCodec.encode(reducedValue));
        }
        // For op 0, 1, 2, all need "receiving"
        if (op[1] != -1) {
          // Receive data
          GroupCommMessage msg = null;
          do {
            msg = receiveMsg(iteration, driverID, taskID);
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
                return 1;
              } else {
                msg = null;
              }
            } else if (msg.getType() == Type.SourceAdd) {
              // Source Add message from the driver
              // ignore the message mistakenly put here
              if (msg.getSrcVersion() == version) {
                return 3;
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
          sendMsg(Type.AllReduce, selfID, taskID, iteration, EmptyByteArr);
        }
        opPerformed.put(op[0], op[1]);
      }
    }
    // Success
    return 0;
  }

  @SuppressWarnings("unused")
  private void propagateFailure(int iteration, String selfID,
    List<int[]> opDimList, Map<Integer, String> opTaskMap,
    Map<Integer, Integer> opPerformed) throws NetworkException,
    InterruptedException, IOException {
    String taskID = null;
    int[] op = null;
    for (int i = 0; i < opDimList.size(); i++) {
      op = opDimList.get(i);
      if (!opPerformed.containsKey(op[0])) {
        // If op is not performed, continue
        taskID = opTaskMap.get(op[0]);
        System.out.println("Propagate failure, Task ID: " + taskID
          + ", op ID: " + op[1]);
        if (op[1] == 1 || op[1] == 2) {
          // Send message to a task, use version as iteration .
          sendMsg(Type.SourceDead, selfID, taskID, iteration, EmptyByteArr);
        }
        // No data receiving
        // If a node is in applying, once it receive the data it will start to
        // propagate failure. If a node already in failure propagation, it
        // ignores the data sent to itself
        if (op[1] == 0) {
          // Send message to a task, use version as iteration.
          sendMsg(Type.SourceDead, selfID, taskID, iteration, EmptyByteArr);
        }
        opPerformed.put(op[0], op[1]);
      }
    }
  }

  private void sendMsg(Type msgType, String selfID, String taskID, int version,
    byte[] bytes) {
    try {
      sender.send(Utils.bldVersionedGCM(groupName, operName, msgType, selfID,
        version, taskID, version, bytes));
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

  private GroupCommMessage receiveMsg(int iteration, String driverID,
    String neighborID) throws InterruptedException {
    GroupCommMessage msg = null;
    do {
      msg = getMsgFromMap(iteration, neighborID);
      if (msg != null) {
        return msg;
      }
      msg = dataQueue.take();
      if (msg.getType() == Type.SourceDead) {
        return msg;
      } else if (msg.getType() == Type.SourceAdd) {
        return msg;
      } else if (msg.getType() == Type.AllReduce) {
        // For AllReduce type message, version is used as iteration
        if (msg.getSrcid().equals(neighborID)
          && msg.getSrcVersion() == iteration) {
          return msg;
        } else {
          addMsgToMap(msg);
          msg = null;
        }
      }
    } while (msg == null);
    return msg;
  }

  private GroupCommMessage getMsgFromMap(int version, String taskID) {
    ConcurrentMap<String, GroupCommMessage> versionMap = dataMap.get(version);
    if (versionMap == null) {
      versionMap = new ConcurrentHashMap<>();
      dataMap.putIfAbsent(version, versionMap);
    }
    return versionMap.get(taskID);
  }

  private void addMsgToMap(GroupCommMessage msg) {
    ConcurrentMap<String, GroupCommMessage> versionMap =
      dataMap.get(msg.getSrcVersion());
    if (versionMap == null) {
      versionMap = new ConcurrentHashMap<>();
      ConcurrentMap<String, GroupCommMessage> oldVersionMap =
        dataMap.putIfAbsent(msg.getSrcVersion(), versionMap);
      if (oldVersionMap != null) {
        versionMap = oldVersionMap;
      }
    }
    versionMap.put(msg.getSrcid(), msg);
  }

  @Override
  public T apply(T element, List<? extends Identifier> order)
    throws InterruptedException, NetworkException {
    // Disable this method as what ReduceReceiver does.
    throw new UnsupportedOperationException();
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
}
