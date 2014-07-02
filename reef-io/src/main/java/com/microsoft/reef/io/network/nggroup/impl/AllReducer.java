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
  private ConcurrentMap<Integer, String> opTaskMap;
  private final AtomicBoolean isLastIteFailed = new AtomicBoolean(false);
  private int numFailedIterations = 0;
  private final AtomicBoolean isNewTaskAdded = new AtomicBoolean(false);

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
    LOG.info(operName + " has CommGroupHandler-"
      + commGroupNetworkHandler.toString());
    this.iteration = new AtomicInteger(version);
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
  }

  @Override
  public int getVersion() {
    // Always return 0.
    // Agreed by client and driver in hypercube topology
    // and allreduce related code.
    return 0;
  }

  @Override
  public void initialize() {
    // Due to the control on CommunicationGroupClientImpl initialization
    // This code won't be invoked repeatedly.
    topoClient.getTopology();
    updateTopology();
    iteration.set(topoClient.getNewIteration());
  }

  private void updateTopology() {
    node = topoClient.getNode();
    opTaskMap = topoClient.getOpTaskMap();
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
    // Use HyperCubeTopoClient to handle some messages
    // This method can be invoked simultaneously with apply method
    // If the msg type is TopologyChange or TopologyUpdated
    // they cannot arrive here
    System.out.println(getQualifiedName() + "Handling " + msg.getType()
      + " msg from " + msg.getSrcid() + " with version " + msg.getSrcVersion());
    // Data are put in the queue
    // and wait to be processed in "apply"
    // Topology related messages are processed by topoClient
    if (msg.getType() == Type.AllReduce) {
      dataQueue.add(msg);
    } else if (msg.getType() == Type.SourceDead) {
      // Only driver sends this message
      dataQueue.add(msg);
    } else if (msg.getType() == Type.SourceAdd) {
      // Only driver sends this message
      dataQueue.add(msg);
    } else {
      topoClient.handle(msg);
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
    System.out.println("Current iteration: " + iteration.get() + " "
      + ", current topo client iteration: " + topoClient.getNewIteration());
    int ite = iteration.get();
    boolean isFailed = false;
    // Update topology when either condition comes true
    // We carefully set the conditions to make sure that
    // a node only response to one of the two.
    if (isLastIteFailed.get() || isNewTaskAdded.get()) {
      if (ite == topoClient.getNewIteration()) {
        updateTopology();
      }
    }
    if (isNewTaskAdded.get()) {
      isNewTaskAdded.set(false);
    }
    T reducedValue = aElement;
    List<byte[]> valBytes = new ArrayList<byte[]>();
    List<T> vals = new ArrayList<T>();
    Map<Integer, Integer> opPerformed = new HashMap<>();
    int exitCode = -1;
    for (int i = 0; i < node.getNeighborOpList().size(); i++) {
      exitCode =
        applyInDim(reducedValue, ite, selfID, driverID, valBytes, node
          .getNeighborOpList().get(i), opTaskMap, opPerformed);
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
        sendMsg(Type.SourceDead, selfID, driverID, 0, getIterationInBytes(ite));
        // Get the new topology
        System.out
          .println("Waiting for the new topology after getting SourceDead.");
        topoClient.getTopology();
        int topoIte = topoClient.getNewIteration();
        System.out.println("The current iteration: " + ite
          + ", the iteration of topology got from the driver: " + topoIte);
        // Fail the current iteration
        // This should match with topoClient.isFailed();
        isFailed = true;
        break;
      } else if (exitCode == 3) {
        // Ask driver, send source add message
        sendMsg(Type.SourceAdd, selfID, driverID, 0, getIterationInBytes(ite));
        // Get the new topology
        System.out
          .println("Waiting for the new topology after getting SourceAdd");
        topoClient.getTopology();
        int topoIte = topoClient.getNewIteration();
        System.out.println("The current iteration: " + ite
          + ", the iteration of topology got from the driver: " + topoIte);
        // Check if there is failure in the current topology
        isFailed = topoClient.isFailed();
        if (isFailed) {
          break;
        } else {
          isNewTaskAdded.set(true);
        }
        // Restart this dimension
        // continue with the ops not performed
        i--;
      }
    }
    if (isFailed) {
      reducedValue = null;
      // Remove received byte data between this iteration and the new iteration
      for (int i = iteration.get(); i < topoClient.getNewIteration(); i++) {
        dataMap.remove(i);
      }
      isLastIteFailed.set(true);
      numFailedIterations = iteration.get() - topoClient.getBaseIteration() + 1;
      iteration.set(topoClient.getNewIteration());
    } else {
      dataMap.remove(iteration.get());
      isLastIteFailed.set(false);
      numFailedIterations = 0;
      iteration.incrementAndGet();
    }
    return reducedValue;
  }

  private int applyInDim(T reducedValue, int iteration, String selfID,
    String driverID, List<byte[]> valBytes, List<int[]> opDimList,
    Map<Integer, String> opTaskMap, Map<Integer, Integer> opPerformed)
    throws NetworkException, InterruptedException {
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
          GroupCommMessage msg = receiveMsg(iteration, driverID, taskID);
          if (msg.getType() == Type.AllReduce) {
            bytes = Utils.getData(msg);
            // If op is 0 or 2, the data is needed
            if (op[1] != 1) {
              valBytes.add(bytes);
            }
          } else if (msg.getType() == Type.SourceDead) {
            // Source Dead message from driver
            return 1;
          } else if (msg.getType() == Type.SourceAdd) {
            // Source Add message from the driver
            return 3;
          }
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
      dataMap.putIfAbsent(msg.getSrcVersion(), versionMap);
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
