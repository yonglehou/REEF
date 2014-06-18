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

import java.util.ArrayList;
import java.util.List;
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
  // Version is used for iteration
  private AtomicInteger version;

  private final ConcurrentMap<Integer, ConcurrentMap<String, BlockingQueue<GroupCommMessage>>> dataMap =
    new ConcurrentHashMap<>();
  private final Object lockObject = new Object();

  private String selfID;
  private HyperCubeNode node;
  private ConcurrentMap<Integer, String> opTaskMap;

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
    this.version = new AtomicInteger(version);
    LOG.info(operName + " has CommGroupHandler-"
      + commGroupNetworkHandler.toString());
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
  }

  @Override
  public int getVersion() {
    return version.get();
  }

  @Override
  public void initialize() {
    // Due to the control on CommunicationGroupClientImpl initialization
    // This code won't be invoked repeatedly.
    topoClient.initialize();
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
    // Use HyperCubeTopoClient to handle the message
    if (msg.getType() == Type.AllReduce) {
      System.out.println(getQualifiedName() + "Handling " + msg.getType()
        + " msg from " + msg.getSrcid());
      putData(msg);
    } else {
      topoClient.handle(msg);
    }
  }

  /**
   * Store the data to the map when data is coming.
   * 
   * @param msg
   */
  public void putData(final GroupCommMessage msg) {
    BlockingQueue<GroupCommMessage> dataQueue =
      getDataQueueFromMap(msg.getVersion(), msg.getSrcid());
    dataQueue.add(msg);
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
    System.out.println("Current version: " + version.get());
    T reducedValue = aElement;
    List<byte[]> valBytes = new ArrayList<byte[]>();
    List<T> vals = new ArrayList<T>();
    List<List<int[]>> opList = node.getNeighborOpList();
    List<int[]> opDimList = null;
    int[] op = null;
    String taskID = null;
    for (int i = 0; i < opList.size(); i++) {
      opDimList = opList.get(i);
      for (int j = 0; j < opDimList.size(); j++) {
        op = opDimList.get(j);
        taskID = opTaskMap.get(op[0]);
        System.out.println("Task ID: " + taskID + ", op ID: " + op[1]);
        if (op[1] == 0) {
          valBytes.add(receiveData(taskID));
          sendData(EmptyByteArr, taskID);
        } else if (op[1] == 1) {
          sendData(dataCodec.encode(reducedValue), taskID);
          receiveData(taskID);
        } else if (op[1] == 2) {
          sendData(dataCodec.encode(reducedValue), taskID);
          valBytes.add(receiveData(taskID));
        } else {
          // No action
        }
      }
      vals.add(reducedValue);
      for (byte[] data : valBytes) {
        vals.add(dataCodec.decode(data));
      }
      reducedValue = reduceFunction.apply(vals);
      valBytes.clear();
      vals.clear();
    }
    version.incrementAndGet();
    return reducedValue;
  }

  @Override
  public T apply(T element, List<? extends Identifier> order)
    throws InterruptedException, NetworkException {
    // Disable this method as what ReduceReceiver does.
    throw new UnsupportedOperationException();
  }

  private void sendData(byte[] data, String neighborID) throws NetworkException {
    sender.send(Utils.bldVersionedGCM(groupName, operName, Type.AllReduce,
      node.getTaskID(), version.get(), neighborID, version.get(), data));
  }

  private byte[] receiveData(String neighborID) throws InterruptedException {
    BlockingQueue<GroupCommMessage> dataQueue =
      getDataQueueFromMap(version.get(), neighborID);
    return Utils.getData(dataQueue.take());
    // We need to be careful about this "take" operation
    // It will block the execution
    // We may want to receive other messages for furthur control
  }
  
  private BlockingQueue<GroupCommMessage> getDataQueueFromMap(int version,
    String taskID) {
    ConcurrentMap<String, BlockingQueue<GroupCommMessage>> versionMap =
      dataMap.get(version);
    if (versionMap == null) {
      versionMap = new ConcurrentHashMap<>();
      dataMap.putIfAbsent(version, versionMap);
    }
    BlockingQueue<GroupCommMessage> dataQueue = versionMap.get(taskID);
    if (dataQueue == null) {
      dataQueue = new LinkedBlockingQueue<>();
      versionMap.putIfAbsent(taskID, dataQueue);
    }
    return dataQueue;
  }

  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName) + ":"
      + selfID + " - ";
  }
}
