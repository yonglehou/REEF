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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
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
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.impl.ThreadPoolStage;

public class AllReducer<T> implements AllReduce<T>,
  EventHandler<GroupCommMessage> {

  private static final Logger LOG = Logger
    .getLogger(AllReducer.class.getName());
  // private static final byte[] EmptyByteArr = new byte[0];
  
  private static final int SMALL_MSG_SIZE = 1024;

  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final CommGroupNetworkHandler commGroupNetworkHandler;

  private final NetworkService<GroupCommMessage> netService;
  private final Sender sender;
  private final Codec<T> dataCodec;
  private final ReduceFunction<T> reduceFunction;
  private final String selfID;
  private final String driverID;
  private final int version;

  private final HyperCubeTopoClient topoClient;

  // Data map for normal allreduce, allreduce with reducescatter + allgather
  private final ConcurrentMap<Integer, ConcurrentMap<Integer, ConcurrentMap<String, LinkedList<GroupCommMessage>>>> dataMap;
  // Message queue
  private final BlockingQueue<GroupCommMessage> dataQueue;

  // Topology structure of this node
  private HyperCubeNode node;
  private Map<Integer, String> nodeTaskMap;
  private Map<String, Integer> taskVersionMap;

  // Flow control
  private final AtomicBoolean isTopoInitialized;
  private final AtomicBoolean isTopoUpdating;
  private final CyclicBarrier topoBarrier;
  // The status of the main thread, either doing "apply" or "checkIteration"
  private final Object runningLock;
  private final AtomicBoolean isRunning;
  private final AtomicBoolean isWaiting;
  private final AtomicBoolean isIterationChecking;
  private final AtomicInteger iteration;
  private final AtomicInteger iteCommID;

  // Failure control
  private final AtomicBoolean isCurrentIterationFailed;
  private final AtomicBoolean isNewTaskComing;
  private final AtomicInteger nextIteration;

  private final EStage<GroupCommMessage> senderStage;

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
    printLog(operName + " is initializing");
    this.version = version;
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

    dataMap = new ConcurrentHashMap<>();
    dataQueue = new LinkedBlockingQueue<>();

    // Flow control
    isTopoInitialized = new AtomicBoolean(false);
    isTopoUpdating = new AtomicBoolean(false);
    isRunning = new AtomicBoolean(false);
    isWaiting = new AtomicBoolean(false);
    iteration = new AtomicInteger(0);
    iteCommID = new AtomicInteger(0);
    topoBarrier = new CyclicBarrier(2);
    runningLock = new Object();
    isIterationChecking = new AtomicBoolean(false);

    // Failure control
    isCurrentIterationFailed = new AtomicBoolean(false);
    // The iteration task needs to jump to after failure.
    nextIteration = new AtomicInteger(0);
    isNewTaskComing = new AtomicBoolean(false);

    senderStage =
      new ThreadPoolStage<>(operName + "SenderStage",
        new EventHandler<GroupCommMessage>() {
          @Override
          public void onNext(final GroupCommMessage msg) {
            try {
              printMsgInfo(msg, "Send the message in SenderStage");
              sender.send(msg);
              printMsgInfo(msg, "Succeed sending the message in SenderStage");
            } catch (Throwable t) {
              printMsgInfo(msg, "Fail to send the message in SenderStage");
              t.printStackTrace(System.out);
              LOG.log(Level.INFO, "Fail to send the message in SenderStage", t);
            }
          }
        }, 1);
    printLog(operName + " is initialized.");
  }

  @Override
  public ReduceFunction<T> getReduceFunction() {
    return reduceFunction;
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
    // This code may be invoked in the first allreduce apply call or
    // invoked by other operators in initialization.
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
    // printMsgInfo(msg, "Enter onNext.");
    if (msg.getVersion() != version) {
      printMsgInfo(msg, "WRONG VERSION.");
      return;
    }
    // Use runningLock to sync with startApplying
    // and stopApplying. If apply is running, this will
    // put messages to the queue. If apply is stopped, this will process
    // messages by itself.
    synchronized (runningLock) {
      if (isRunning.get()) {
        if (msg.getType() == Type.AllReduce || msg.getType() == Type.SourceDead
          || msg.getType() == Type.SourceAdd) {
          printMsgInfo(msg, "Send msg to the data queue.");
          dataQueue.add(msg);
        } else if (msg.getType() == Type.UpdateTopology) {
          // Topology related messages are processed by topoClient
          // No Topology setup, it should not show when apply is running.
          printMsgInfo(msg, "Send msg to the ctrl queue.");
          topoClient.handle(msg);
        } else {
          printMsgInfo(msg, "Ignore.");
          return;
        }
      } else {
        processMsgInIdle(msg);
      }
    }
    // printMsgInfo(msg, "Leave onNext.");
  }

  private void processMsgInIdle(GroupCommMessage msg) {
    // Process the messages when no "applying" happens
    // Sync through runningLock
    if (msg.getVersion() != version) {
      // When this message is processed, check the version again.
      printMsgInfo(msg, "Wrong version found when processing in idle.");
      return;
    }
    if (msg.getType() == Type.TopologySetup
      || msg.getType() == Type.UpdateTopology) {
      processOneMsgInIdle(msg);
      // Unblock apply if it is blocked by topology initialization or updating.
      if (isWaiting.get()) {
        try {
          topoBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
          e.printStackTrace();
        }
      } else {
        // If isWaiting, no need to continue processing, let the main thread
        // process.
        while (!dataQueue.isEmpty()) {
          try {
            boolean goOn = processOneMsgInIdle(dataQueue.take());
            if (!goOn) {
              break;
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    } else {
      // Process other type of messages.
      processOneMsgInIdle(msg);
    }
  }

  private boolean processOneMsgInIdle(GroupCommMessage msg) {
    if (msg.getVersion() != version) {
      // When this message is processed, check the version again.
      printMsgInfo(msg,
        "Wrong version found when processing one message in idle.");
      // Continue to next message
      return true;
    }
    if (msg.getType() == Type.AllReduce) {
      if (!isTopoInitialized.get() || isTopoUpdating.get()) {
        printMsgInfo(msg, "Put the msg to the data queue in idle.");
        dataQueue.add(msg);
        return false;
      } else {
        // No "applying" is working, put to the map
        int[] iteComm = getIterationAndCommID(msg);
        int msgIteration = iteComm[0];
        int msgCommType = iteComm[1];
        printMsgInfo(msg, "Add the msg to the map in idle, iteration: "
          + msgIteration + ", commID: " + msgCommType);
        addMsgToMap(msg, msgIteration, msgCommType);
      }
    } else if (msg.getType() == Type.SourceDead
      || msg.getType() == Type.SourceAdd) {
      if (!isTopoInitialized.get() || isTopoUpdating.get() || isWaiting.get()) {
        // Driver won't send new message if the former one is not processed.
        // So there is only one such a message in the queue.
        // If the message was in the queue, it is added back.
        printMsgInfo(msg, "Put the msg in idle to the data queue.");
        dataQueue.add(msg);
        return false;
      } else {
        printMsgInfo(msg, "Process the msg in idle.");
        GroupCommMessage ack =
          Utils.bldVersionedGCM(groupName, operName, msg.getType(), selfID,
            version, driverID, version, putIterationToBytes(iteration.get()));
        senderStage.onNext(ack);
        isTopoUpdating.set(true);
        return false;
      }
    } else if (msg.getType() == Type.TopologySetup) {
      // Initialize if is never initialized.
      if (isTopoInitialized.compareAndSet(false, true)) {
        // topoClient.handle(msg);
        printMsgInfo(msg, "Process the msg in idle.");
        initializeNodeTopologyInIdle(msg, topoClient);
      } else {
        printMsgInfo(msg, "Ignore");
      }
    } else if (msg.getType() == Type.UpdateTopology) {
      printMsgInfo(msg, "Process the msg in idle.");
      // This message won't be generated unless Source add/dead
      // are processed. Once this message is received, process immediately.
      getNewNodeTopologyInIdle(msg, topoClient, iteration.get());
      // Check if there is failure in the current topology
      examineNodeTopologyFailure();
      isTopoUpdating.set(false);
    }
    return true;
  }

  private void initializeNodeTopologyInIdle(GroupCommMessage msg,
    HyperCubeTopoClient topoClient) {
    // topoClient.waitForNewNodeTopology();
    getNewNodeTopologyInIdle(msg, topoClient, iteration.get());
    // NodeTopology nodeTopo = topoClient.getNewestNodeTopology();
    // This should not be null
    // node = nodeTopo.node;
    // nodeTaskMap = nodeTopo.nodeTaskMap;
    // taskVersionMap = nodeTopo.taskVersionMap;
    // iteration.set(nodeTopo.newIteration);

    // AllReducer doesn't have the topology at the beginning
    // This can be considered as failure
    isCurrentIterationFailed.set(true);
    nextIteration.set(topoClient.getNewestNodeTopology().newIteration);
    isNewTaskComing.set(false);
    printLog("Node topology is initialized. Next iteration is "
      + nextIteration.get());
  }

  private void getNewNodeTopologyInIdle(GroupCommMessage msg,
    HyperCubeTopoClient topoClient, int currentIteration) {
    topoClient.processNodeTopologyMsg(msg);
    printLog("The current iteration (in idle): " + currentIteration
      + ", the base iteration of topology got from the driver: "
      + topoClient.getNewestNodeTopology().baseIteration
      + ", the new iteration of topology got from the driver: "
      + topoClient.getNewestNodeTopology().newIteration + ", is failed? "
      + topoClient.getNewestNodeTopology().isFailed);
  }

  private void examineNodeTopologyFailure() {
    boolean isFailed = topoClient.getNewestNodeTopology().isFailed;
    int newIteration = topoClient.getNewestNodeTopology().newIteration;
    if (isFailed) {
      // if (!isCurrentIterationFailed.get()) {
      isCurrentIterationFailed.set(true);
      nextIteration.set(newIteration);
      isNewTaskComing.set(false);
      printLog("Current iteration is failed. Next iteration is "
        + nextIteration.get());
      // }
    }
  }

  private void startWorking() {
    // Block message processing in onNext
    boolean isUpdating = false;
    boolean isInitialized = false;
    // System.out.println("Start Stage 1");
    synchronized (runningLock) {
      // System.out.println("Enter Start Stage 1");
      isWaiting.set(true);
      isUpdating = isTopoUpdating.get();
      isInitialized = isTopoInitialized.get();
      // System.out.println("Leave Start Stage 1");
    }
    // Allow message processing in onNext
    // If onNext see apply is waiting,
    // it stops processing source add/dead
    // and add to the queue.
    // If it is initializing/updating topology
    // wait until topology updating is finished.
    if (!isInitialized || isUpdating) {
      printLog("Wait for topology initialization or update...");
      try {
        topoBarrier.await();
      } catch (InterruptedException | BrokenBarrierException e) {
        e.printStackTrace();
      }
    }
    // Block applying
    // If onNext is updating, let it finish first
    // System.out.println("Start Stage 2");
    synchronized (runningLock) {
      // System.out.println("Enter Start Stage 2");
      isRunning.set(true);
      isWaiting.set(false);
      // System.out.println("Leave Start Stage 2");
    }
  }

  private void finishWorking() {
    // System.out.println("Finish Stage 1");
    synchronized (runningLock) {
      // System.out.println("Enter Fnish Stage 1");
      while (!dataQueue.isEmpty()) {
        try {
          boolean goOn = processOneMsgInIdle(dataQueue.take());
          if (!goOn) {
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      isRunning.set(false);
      // System.out.println("Leave Finish Stage 1");
    }
  }

  @Override
  public synchronized T apply(T aElement) throws InterruptedException,
    NetworkException {
    startWorking();
    if (isCurrentIterationFailed.get()) {
      finishWorking();
      return null;
    }
    int ite = iteration.get();
    int commID = iteCommID.get();
    printLog("Current allreduce iteration: " + ite + " comm ID: " + commID);
    // Prepare applying
    T reducedValue = aElement;
    LinkedList<byte[]> valBytes = new LinkedList<byte[]>();
    LinkedList<T> vals = new LinkedList<T>();
    // Encode iteration and communication type for sending
    byte[] iteComm = putIterationAndCommIDToBytes(ite, commID);
    boolean isFailed = false;
    for (int i = 0; i < node.getNeighborOpList().size(); i++) {
      isFailed =
        applyInDim(reducedValue, valBytes, node.getNeighborOpList().get(i),
          nodeTaskMap, taskVersionMap, selfID, version, ite, commID, iteComm);
      printLog("Finish applying dimension " + i + ". is failed? " + isFailed);
      if (!isFailed) {
        vals.add(reducedValue);
        while (!valBytes.isEmpty()) {
          byte[] bytes = valBytes.removeFirst();
          long time1 = System.currentTimeMillis();
          T val = dataCodec.decode(bytes);
          long time2 = System.currentTimeMillis();
          printLog("Data decode - byte length " + bytes.length + ", took (ms) "
            + (time2 - time1));
          printMemUsage();
          vals.add(val);
        }
        long time1 = System.currentTimeMillis();
        reducedValue = reduceFunction.apply(vals);
        long time2 = System.currentTimeMillis();
        printLog("Reduce on dimension  " + i + ", took (ms) " + (time2 - time1));
        printMemUsage();
        // Reset
        valBytes.clear();
        vals.clear();
      } else {
        valBytes.clear();
        break;
      }
    }
    if (isFailed) {
      // Mark the current iteration as failed
      reducedValue = null;
      examineNodeTopologyFailure();
    } else {
      // If this "apply" operation succeed
      // increase commID
      iteCommID.incrementAndGet();
    }
    finishWorking();
    return reducedValue;
  }

  private boolean applyInDim(T reducedValue, List<byte[]> valBytes,
    List<int[]> opDimList, Map<Integer, String> opTaskMap,
    Map<String, Integer> taskVersionMap, String selfID, int version,
    int iteration, int commID, byte[] iteComm) throws NetworkException,
    InterruptedException {
    // If apply is failed, return true, else return false
    for (int i = 0; i < opDimList.size(); i++) {
      int[] op = opDimList.get(i);
      String taskID = opTaskMap.get(op[0]);
      printLog("Apply Task ID: " + taskID + ", op: [" + op[0] + "," + op[1]
        + "]");
      byte[] bytes = null;
      boolean dataSent = false;
      boolean dataRecvd = false;
      if (op[1] == 1 || op[1] == 2) {
        // Send data to other tasks
        // A byte is used for data info
        // 1 means data sending request
        // 0 means real data
        // -1 means data sending reply
        long time1 = System.currentTimeMillis();
        bytes = dataCodec.encode(reducedValue);
        long time2 = System.currentTimeMillis();
        printLog("Data encode - byte length " + bytes.length + ", took (ms) "
          + (time2 - time1));
        printMemUsage();
        if (bytes.length > SMALL_MSG_SIZE) {
          printLog("Send data sending request");
          sendMsg(Type.AllReduce, selfID, version, taskID,
            taskVersionMap.get(taskID), iteComm, new byte[] { 1 });
        } else {
          printLog("Send full data.");
          sendMsg(Type.AllReduce, selfID, version, taskID,
            taskVersionMap.get(taskID), iteComm, new byte[] { 0 }, bytes);
          dataSent = true;
        }
      }
      // op 0 need "receiving"
      // op 1 need "receiving" if the real data is not sent
      // op 2 needs "sending and receiving"
      while ((op[1] == 0 && !dataRecvd) || (op[1] == 1 && !dataSent)
        || (op[1] == 2 && !(dataSent && dataRecvd))) {
        // Receive data with expected sender task ID, iteration ID,
        // communication ID and version
        GroupCommMessage msg = receiveMsg(iteration, taskID, commID, version);
        if (msg.getType() == Type.AllReduce) {
          byte[] dataInfo = msg.getMsgs(1).getData().toByteArray();
          if (dataInfo[0] == 0) {
            // If data info is 0
            // Receive data and set receive to true
            byte[] recvBytes = msg.getMsgs(2).getData().toByteArray();
            printLog("Receive byte data with length " + recvBytes.length);
            valBytes.add(recvBytes);
            dataRecvd = true;
          } else if (dataInfo[0] == 1) {
            // If data info is 1
            // Send a message with data info -1 to ack the sending request
            printLog("Send data sending request ack.");
            sendMsg(Type.AllReduce, selfID, version, taskID,
              taskVersionMap.get(taskID), iteComm, new byte[] { -1 });
          } else if (dataInfo[0] == -1) {
            // If data info is -1
            // Send the data and set send to true
            printLog("Send full data.");
            sendMsg(Type.AllReduce, selfID, version, taskID,
              taskVersionMap.get(taskID), iteComm, new byte[] { 0 }, bytes);
            dataSent = true;
          }
        } else if (msg.getType() == Type.SourceDead) {
          // Ask driver, send source dead message
          // The version number is a number agreed between driver and task
          sendMsg(Type.SourceDead, selfID, version, driverID, version,
            putIterationToBytes(iteration));
          // Get the new topology
          waitForNewNodeTopology(topoClient, iteration, Type.SourceDead);
          return true;
        } else if (msg.getType() == Type.SourceAdd) {
          // Source Add message from the driver
          // Ask driver, send source add message
          sendMsg(Type.SourceAdd, selfID, version, driverID, version,
            putIterationToBytes(iteration));
          // Get the new topology
          waitForNewNodeTopology(topoClient, iteration, Type.SourceAdd);
          // Check if there is failure in the current topology
          boolean isFailed = topoClient.getNewestNodeTopology().isFailed;
          if (isFailed) {
            return true;
          } else {
            // Continue receiving
          }
        }
      }
    }
    // Success
    return false;
  }

  private void updateNodeTopology(int iteration) {
    // If there is no new topology, keep the old one.
    NodeTopology nodeTopo = topoClient.getNodeTopology(iteration);
    if (nodeTopo != null) {
      node = nodeTopo.node;
      nodeTaskMap = nodeTopo.nodeTaskMap;
      taskVersionMap = nodeTopo.taskVersionMap;
      // If update successful, remove the topologies whose iteration number is
      // lower than the current one
      List<Integer> rmIterations =
        topoClient.removeOldNodeTopologies(iteration);
      StringBuffer sb = new StringBuffer();
      for (int rmIte : rmIterations) {
        sb.append(rmIte + ",");
      }
      if (sb.length() == 0) {
        sb.append("none.");
      } else {
        sb.setCharAt(sb.length() - 1, '.');
      }
      printLog("Current new topology with iteration " + iteration
        + ". Remove topologies with iterations: " + sb);
    }
  }

  private void waitForNewNodeTopology(HyperCubeTopoClient topoClient,
    int currentIteration, Type msgType) {
    printLog("Waiting for the new topology after getting " + msgType + ".");
    topoClient.waitForNewNodeTopology();
    printLog("The current iteration: " + currentIteration
      + ", the base iteration of topology got from the driver: "
      + topoClient.getNewestNodeTopology().baseIteration
      + ", the new iteration of topology got from the driver: "
      + topoClient.getNewestNodeTopology().newIteration + ", is failed? "
      + topoClient.getNewestNodeTopology().isFailed);
  }

  private void sendMsg(Type msgType, String selfID, int srcVersion,
    String taskID, int tgtVersion, byte[]... bytes) {
    GroupCommMessage msg =
      Utils.bldVersionedGCM(groupName, operName, msgType, selfID, srcVersion,
        taskID, tgtVersion, bytes);
    senderStage.onNext(msg);
    // try {
    // printMsgInfo(msg, "Send the message in main.");
    // sender.send(msg);
    // } catch (Exception e) {
    // printMsgInfo(msg, "Fail to send the message in main.");
    // e.printStackTrace();
    // }
  }

  private byte[] putIterationToBytes(int iteration) {
    ByteArrayOutputStream bout = new ByteArrayOutputStream(4);
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

  private byte[] putIterationAndCommIDToBytes(int iteration, int commType) {
    ByteArrayOutputStream bout = new ByteArrayOutputStream(8);
    DataOutputStream dout = new DataOutputStream(bout);
    byte[] bytes = null;
    try {
      dout.writeInt(iteration);
      dout.writeInt(commType);
      dout.flush();
      dout.close();
      bytes = bout.toByteArray();
    } catch (IOException e) {
      bytes = null;
      e.printStackTrace();
    }
    return bytes;
  }

  private int[] getIterationAndCommID(GroupCommMessage msg) {
    int[] iteComm = new int[2];
    byte[] bytes = msg.getMsgs(0).getData().toByteArray();
    ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
    DataInputStream din = new DataInputStream(bin);
    try {
      iteComm[0] = din.readInt();
      iteComm[1] = din.readInt();
      din.close();
    } catch (IOException e) {
      iteComm = null;
      e.printStackTrace();
    }
    return iteComm;
  }

  private GroupCommMessage receiveMsg(int iteration, String neighborID,
    int commID, int version) throws InterruptedException {
    // We use commID to mark the number of rounds of walking through the
    // topology. Normal "apply" uses one ID, ReduceScatter uses one ID, and
    // Allgather uses another ID.
    GroupCommMessage msg = null;
    do {
      msg = getMsgFromMap(iteration, commID, neighborID, version);
      if (msg != null) {
        return msg;
      }
      msg = dataQueue.take();
      if (msg.getVersion() != version) {
        msg = null;
      } else if (msg.getType() == Type.SourceDead) {
        printMsgInfo(msg, "Process from the queue.");
        return msg;
      } else if (msg.getType() == Type.SourceAdd) {
        printMsgInfo(msg, "Process from the queue.");
        return msg;
      } else if (msg.getType() == Type.AllReduce) {
        // Source version is used as iteration
        // Target version is used as communication type
        int[] iteComm = getIterationAndCommID(msg);
        int msgIteration = iteComm[0];
        int msgCommID = iteComm[1];
        if (msg.getSrcid().equals(neighborID) && msgIteration == iteration
          && msgCommID == commID) {
          printMsgInfo(msg, "Get the message - iteration: " + msgIteration
            + ", commID: " + msgCommID);
          return msg;
        } else {
          // Notice that if the msg version is incorrect,
          // it cannot be added to the map.
          printMsgInfo(msg, "Add the message to map - iteration: "
            + msgIteration + ", commID: " + msgCommID);
          addMsgToMap(msg, msgIteration, msgCommID);
          msg = null;
        }
      }
    } while (msg == null);
    return msg;
  }

  private GroupCommMessage getMsgFromMap(int iteration, int commID,
    String taskID, int version) {
    LinkedList<GroupCommMessage> msgList =
      getMsgList(iteration, commID, taskID);
    GroupCommMessage msg = null;
    synchronized (msgList) {
      if (!msgList.isEmpty()) {
        msg = msgList.removeFirst();
      }
    }
    // We may or may not get a message
    // Avoid old version message
    if (msg != null && msg.getVersion() == version) {
      return msg;
    } else {
      return null;
    }
  }

  private void addMsgToMap(GroupCommMessage msg, int iteration, int commID) {
    LinkedList<GroupCommMessage> msgList =
      getMsgList(iteration, commID, msg.getSrcid());
    synchronized (msgList) {
      msgList.add(msg);
    }
  }

  private void removeOldMsgInMap(int iteration) {
    // Remove the messages from the iteration with iteration number less than
    // the current one.
    List<Integer> keys = new LinkedList<>();
    StringBuffer sb = new StringBuffer();
    for (Entry<Integer, ConcurrentMap<Integer, ConcurrentMap<String, LinkedList<GroupCommMessage>>>> entry : dataMap
      .entrySet()) {
      if (entry.getKey() < iteration) {
        keys.add(entry.getKey().intValue());
        sb.append(entry.getKey().toString() + ",");
      }
    }
    for (int key : keys) {
      dataMap.remove(key);
    }
    if (sb.length() == 0) {
      sb.append("none.");
    } else {
      sb.setCharAt(sb.length() - 1, '.');
    }
    printLog("Remove msg in dataMap from iteration: " + sb);
  }

  private LinkedList<GroupCommMessage> getMsgList(int iteration, int commID,
    String taskID) {
    ConcurrentMap<Integer, ConcurrentMap<String, LinkedList<GroupCommMessage>>> iteMap =
      dataMap.get(iteration);
    if (iteMap == null) {
      iteMap = new ConcurrentHashMap<>();
      ConcurrentMap<Integer, ConcurrentMap<String, LinkedList<GroupCommMessage>>> oldIteMap =
        dataMap.putIfAbsent(iteration, iteMap);
      if (oldIteMap != null) {
        iteMap = oldIteMap;
      }
    }
    ConcurrentMap<String, LinkedList<GroupCommMessage>> taskMsgMap =
      iteMap.get(commID);
    if (taskMsgMap == null) {
      taskMsgMap = new ConcurrentHashMap<>();
      ConcurrentMap<String, LinkedList<GroupCommMessage>> oldTaskMsgMap =
        iteMap.putIfAbsent(commID, taskMsgMap);
      if (oldTaskMsgMap != null) {
        taskMsgMap = oldTaskMsgMap;
      }
    }
    LinkedList<GroupCommMessage> msgList = taskMsgMap.get(taskID);
    if (msgList == null) {
      msgList = new LinkedList<>();
      LinkedList<GroupCommMessage> oldMsgList =
        taskMsgMap.putIfAbsent(taskID, msgList);
      if (oldMsgList != null) {
        msgList = oldMsgList;
      }
    }
    return msgList;
  }

  public synchronized List<T> apply(List<T> elements)
    throws InterruptedException, NetworkException {
    // We need to change the interface to make chunking automatic.
    // Assume the elements are ordered.
    startWorking();
    if (isCurrentIterationFailed.get()) {
      finishWorking();
      return null;
    }
    int ite = iteration.get();
    int commID = iteCommID.get();
    printLog("Current allreduce (reducescatter) iteration: " + ite
      + " commID: " + commID);
    boolean isFailed = false;
    // updateNodeTopology(ite);
    // Data structure for allreduce
    Map<Integer, T> reducedValMap = new TreeMap<>();
    for (int i = 0; i < elements.size(); i++) {
      reducedValMap.put(i, elements.get(i));
    }
    printLog("Num of chunks of ReduceScatter + Allgather (start): "
      + reducedValMap.size());
    // Data structure to collect allreduce results
    List<T> reducedVals = null;
    // Do reduce scatter
    isFailed =
      reduceScatter(reducedValMap, node.getNeighborOpList(), nodeTaskMap,
        taskVersionMap, dataCodec, reduceFunction, selfID, node.getNodeID(),
        version, ite, commID, driverID, topoClient);
    // Do allgather
    if (!isFailed) {
      commID++;
      printLog("Current allreduce (allgather) iteration: " + ite + " commID: "
        + commID);
      isFailed =
        allGather(reducedValMap, node.getNeighborOpList(), nodeTaskMap,
          taskVersionMap, dataCodec, reduceFunction, selfID, node.getNodeID(),
          version, ite, commID, driverID, topoClient);
    }
    if (isFailed) {
      reducedVals = null;
      examineNodeTopologyFailure();
    } else {
      // Remove the reduced vals in the map and put to the list.
      // Keep the order.
      printLog("Num of chunks of ReduceScatter + Allgather (end): "
        + reducedValMap.size());
      reducedVals = new ArrayList<>(reducedValMap.size());
      for (Entry<Integer, T> entry : reducedValMap.entrySet()) {
        reducedVals.add(entry.getKey().intValue(), entry.getValue());
      }
      reducedValMap.clear();
      iteCommID.addAndGet(2);
    }
    finishWorking();
    return reducedVals;
  }

  private boolean reduceScatter(Map<Integer, T> reducedValMap,
    List<List<int[]>> opList, Map<Integer, String> opTaskMap,
    Map<String, Integer> taskVersionMap, Codec<T> codec,
    ReduceFunction<T> reduceFunc, String selfID, int nodeID, int version,
    int iteration, int commID, String driverID, HyperCubeTopoClient topoClient)
    throws InterruptedException {
    // Chunk ID <-> byte messages from other nodes
    Map<Integer, List<byte[]>> valByteMap = new HashMap<>();
    // Used in reduce function
    List<T> vals = new LinkedList<T>();
    // Record operations performed on this dimension
    int[] lastPerformedOpIndex = new int[] { -1 };
    // Encode iteration and communication type for sending
    byte[] iteComm = putIterationAndCommIDToBytes(iteration, commID);
    boolean isFailed = false;
    int moduloBase = 2;
    int exitCode = -1;
    // Do reduce scatter
    for (int i = 0; i < opList.size(); i++) {
      // For each dimension, follow the topology and send the data
      exitCode =
        reduceScatterInDim(reducedValMap, valByteMap, opList.get(i), opTaskMap,
          taskVersionMap, lastPerformedOpIndex, moduloBase, codec, selfID,
          nodeID, version, iteration, commID, iteComm);
      if (exitCode == 0) {
        // Apply on each chunk ID
        printLog("ReduceScatter - Num of chunks received: " + valByteMap.size());
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
          putIterationToBytes(iteration));
        // Get the new topology
        waitForNewNodeTopology(topoClient, iteration, Type.SourceDead);
        // Fail the current iteration
        // This should match with topoClient.isFailed();
        isFailed = true;
        break;
      } else if (exitCode == 2) {
        // Ask driver, send source add message
        sendMsg(Type.SourceAdd, selfID, version, driverID, version,
          putIterationToBytes(iteration));
        // Get the new topology
        waitForNewNodeTopology(topoClient, iteration, Type.SourceAdd);
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

  private int reduceScatterInDim(Map<Integer, T> reducedValMap,
    Map<Integer, List<byte[]>> valByteMap, List<int[]> opDimList,
    Map<Integer, String> opTaskMap, Map<String, Integer> taskVersionMap,
    int[] lastPerformedOpIndex, int moduloBase, Codec<T> codec, String selfID,
    int nodeID, int version, int iteration, int commID, byte[] iteComm)
    throws InterruptedException {
    int lastIndex = lastPerformedOpIndex[0];
    // If the last Performed OP index is less than 0 (negative, means nothing
    // performed), i starts with 0. otherwise, it starts with the last performed
    // op index.
    if (lastIndex >= 0) {
      printLog("Resume ReduceScatter on op with index " + lastIndex);
    }
    int i = lastIndex < 0 ? 0 : lastIndex;
    for (; i < opDimList.size(); i++) {
      int[] op = opDimList.get(i);
      String taskID = opTaskMap.get(op[0]);
      printLog("ReduceScatter Task ID: " + taskID + ", op: [" + op[0] + ","
        + op[1] + "]");
      if (i != lastIndex) {
        // If op is not last op, do as normal
        // otherwise, if the code exits because of ResourceDead and ResourceAdd
        // we record the index and resume receiving.
        if (op[1] == 1 || op[1] == 2) {
          List<Integer> chunkIDs = new LinkedList<>();
          LinkedList<byte[]> byteList = new LinkedList<>();
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
          printLog("ReduceScatter Num of chunks sent: " + chunkIDs.size());
          byteList.addFirst(putChunkIDsToBytes(chunkIDs));
          byteList.addFirst(iteComm);
          sendMsg(Type.AllReduce, selfID, version, taskID,
            taskVersionMap.get(taskID),
            byteList.toArray(new byte[byteList.size()][]));
        }
      }
      // For op 0, 1, 2, all need "receiving"
      if (op[1] != -1) {
        GroupCommMessage msg = null;
        do {
          // Reduce Scatter in Allreduce uses commType 1
          msg = receiveMsg(iteration, taskID, commID, version);
          if (msg.getType() == Type.AllReduce) {
            // If op is 0 or 2, the data is needed
            if (op[1] != 1) {
              getRsAgDataFromMsg(valByteMap, msg);
            }
          } else if (msg.getType() == Type.SourceDead) {
            // Source Dead message from driver
            lastPerformedOpIndex[0] = i;
            return 1;
          } else if (msg.getType() == Type.SourceAdd) {
            // Source Add message from the driver
            lastPerformedOpIndex[0] = i;
            return 2;
          }
        } while (msg == null);
      }
      // If there is no failure in receiving,
      // we continue the original operations.
      // For receiving only operation, send an ack message.
      if (op[1] == 0) {
        // send an ack message to the sender task
        // The source version is the iteration
        sendMsg(Type.AllReduce, selfID, version, taskID,
          taskVersionMap.get(taskID), iteComm);
      }
    }
    // All ops are performed
    lastPerformedOpIndex[0] = opDimList.size();
    // Success
    return 0;
  }

  private byte[] putChunkIDsToBytes(List<Integer> chunkIDs) {
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

  private List<Integer> getChunkIDs(byte[] bytes) {
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

  private void getRsAgDataFromMsg(Map<Integer, List<byte[]>> valByteMap,
    GroupCommMessage msg) {
    List<Integer> chunkIDs =
      getChunkIDs(msg.getMsgs(1).getData().toByteArray());
    int chunkID = -1;
    for (int i = 0; i < chunkIDs.size(); i++) {
      chunkID = chunkIDs.get(i);
      List<byte[]> byteList = valByteMap.get(chunkID);
      if (byteList == null) {
        // Use linked list for the list with unknown size
        byteList = new LinkedList<>();
        valByteMap.put(chunkID, byteList);
      }
      // The first byte[] is iteration and communication type
      // the second byte[] is chunk ids.
      byteList.add(msg.getMsgs(i + 2).getData().toByteArray());
    }
  }

  private boolean allGather(Map<Integer, T> reducedValMap,
    List<List<int[]>> opList, Map<Integer, String> opTaskMap,
    Map<String, Integer> taskVersionMap, Codec<T> codec,
    ReduceFunction<T> reduceFunc, String selfID, int nodeID, int version,
    int iteration, int commID, String driverID, HyperCubeTopoClient topoClient)
    throws InterruptedException {
    // Chunk ID <-> byte messages from other nodes
    Map<Integer, List<byte[]>> valByteMap = new HashMap<>();
    // Used in reduce function
    List<T> vals = new LinkedList<T>();
    // Record last op performed
    int[] lastPerformedOpIndex = new int[] { -1 };
    // Encode iteration and communication type for sending
    byte[] iteComm = putIterationAndCommIDToBytes(iteration, commID);
    boolean isFailed = false;
    int exitCode = -1;
    // Do allgather
    for (int i = 0; i < opList.size(); i++) {
      // For each dimension, follow the topology and send the data
      exitCode =
        allgatherInDim(reducedValMap, valByteMap, opList.get(i), opTaskMap,
          taskVersionMap, lastPerformedOpIndex, codec, selfID, nodeID, version,
          iteration, commID, iteComm);
      if (exitCode == 0) {
        // Apply each chunk received
        printLog("Allgather Num of chunks received: " + valByteMap.size());
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
          putIterationToBytes(iteration));
        // Get the new topology
        waitForNewNodeTopology(topoClient, iteration, Type.SourceDead);
        // Fail the current iteration
        // This should match with topoClient.isFailed();
        isFailed = true;
        break;
      } else if (exitCode == 2) {
        // Ask driver, send source add message
        // The version number is always 0 when sending to the driver
        sendMsg(Type.SourceAdd, selfID, version, driverID, version,
          putIterationToBytes(iteration));
        // Get the new topology
        waitForNewNodeTopology(topoClient, iteration, Type.SourceAdd);
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

  private int allgatherInDim(Map<Integer, T> reducedValMap,
    Map<Integer, List<byte[]>> valByteMap, List<int[]> opDimList,
    Map<Integer, String> opTaskMap, Map<String, Integer> taskVersionMap,
    int[] lastPerformedOpIndex, Codec<T> codec, String selfID, int nodeID,
    int version, int iteration, int commID, byte[] iteComm)
    throws InterruptedException {
    int lastIndex = lastPerformedOpIndex[0];
    // If the last Performed OP index is less than 0 (negative, means nothing
    // performed), i starts with 0. otherwise, it starts with the last performed
    // op index.
    if (lastIndex >= 0) {
      printLog("Resume Allgather on op with index " + lastIndex);
    }
    int i = lastIndex < 0 ? 0 : lastIndex;
    for (; i < opDimList.size(); i++) {
      int[] op = opDimList.get(i);
      String taskID = opTaskMap.get(op[0]);
      printLog("Allgather Task ID: " + taskID + ", op: [" + op[0] + "," + op[1]
        + "]");
      if (i != lastIndex) {
        if (op[1] == 1 || op[1] == 2) {
          List<Integer> chunkIDs = new LinkedList<>();
          LinkedList<byte[]> byteList = new LinkedList<>();
          for (Entry<Integer, T> entry : reducedValMap.entrySet()) {
            int chunkID = entry.getKey().intValue();
            chunkIDs.add(chunkID);
            byteList.add(codec.encode(entry.getValue()));
          }
          printLog("Allgather Num of chunks sent: " + chunkIDs.size());
          byteList.addFirst(putChunkIDsToBytes(chunkIDs));
          byteList.addFirst(iteComm);
          sendMsg(Type.AllReduce, selfID, version, taskID,
            taskVersionMap.get(taskID),
            byteList.toArray(new byte[byteList.size()][]));
        }
      }
      // For op 0, 1, 2, all need "receiving"
      if (op[1] != -1) {
        GroupCommMessage msg = null;
        do {
          // Now we use unified comm ID to mark the different rounds of walking
          // through the topology in one iteration.
          msg = receiveMsg(iteration, taskID, commID, version);
          if (msg.getType() == Type.AllReduce) {
            // If op is 0 or 2, the data is needed
            if (op[1] != 1) {
              getRsAgDataFromMsg(valByteMap, msg);
            }
          } else if (msg.getType() == Type.SourceDead) {
            // Source Dead message from driver
            lastPerformedOpIndex[0] = i;
            return 1;
          } else if (msg.getType() == Type.SourceAdd) {
            // Source Add message from the driver
            // ignore the message mistakenly put here
            lastPerformedOpIndex[0] = i;
            return 2;
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
        sendMsg(Type.AllReduce, selfID, version, taskID,
          taskVersionMap.get(taskID), iteComm);
      }
    }
    lastPerformedOpIndex[0] = opDimList.size();
    // Success
    return 0;
  }

  private void printMsgInfo(GroupCommMessage msg, String cmd) {
    System.out.println(getQualifiedName() + "MSG " + msg.getType() + " from "
      + msg.getSrcid() + " with source version " + msg.getSrcVersion() + " to "
      + msg.getDestid() + " with target version " + msg.getVersion() + ". "
      + cmd);
    LOG.info(getQualifiedName() + "MSG " + msg.getType() + " from "
      + msg.getSrcid() + " with source version " + msg.getSrcVersion() + " to "
      + msg.getDestid() + " with target version " + msg.getVersion() + ". "
      + cmd);
  }

  private void printLog(String log) {
    System.out.println(getQualifiedName() + log);
    LOG.info(getQualifiedName() + log);
  }
  
  private void printMemUsage() {
    long totalMem = Runtime.getRuntime().totalMemory();
    long freeMem = Runtime.getRuntime().freeMemory();
    long usedMem = totalMem - freeMem;
    System.out
      .println("Total Memory (bytes): " + totalMem + " Free Memory (bytes): "
        + freeMem + " Used memory (bytes): " + usedMem);
    LOG.info("Total Memory (bytes): " + totalMem + " Free Memory (bytes): "
      + freeMem + " Used memory (bytes): " + usedMem);
  }

  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName) + ":"
      + selfID + ":" + version + " - ";
  }

  @Override
  public T apply(T element, List<? extends Identifier> order)
    throws InterruptedException, NetworkException {
    // Disable this method as what ReduceReceiver does.
    throw new UnsupportedOperationException();
  }

  public synchronized void checkIteration() {
    startWorking();
    int ite = iteration.get();
    printLog("Check iteration " + ite);
    GroupCommMessage msg = null;
    // Process all the messages in the queue
    while (!dataQueue.isEmpty()) {
      try {
        msg = dataQueue.take();
        if (msg.getType() == Type.AllReduce) {
          int[] iteComm = getIterationAndCommID(msg);
          int msgIteration = iteComm[0];
          int msgCommType = iteComm[1];
          printMsgInfo(msg, "Add msg to the map in idle, iteration: "
            + msgIteration + ", commType: " + msgCommType);
          addMsgToMap(msg, msgIteration, msgCommType);
        } else if (msg.getType() == Type.SourceDead) {
          sendMsg(Type.SourceDead, selfID, version, driverID, version,
            putIterationToBytes(ite));
          waitForNewNodeTopology(topoClient, ite, Type.SourceDead);
          examineNodeTopologyFailure();
        } else if (msg.getType() == Type.SourceDead) {
          sendMsg(Type.SourceAdd, selfID, version, driverID, version,
            putIterationToBytes(ite));
          waitForNewNodeTopology(topoClient, ite, Type.SourceAdd);
          examineNodeTopologyFailure();
        }
      } catch (InterruptedException e) {
        // If exception happens, ignore and continue processing next msg.
        e.printStackTrace();
      }
    }
    isIterationChecking.compareAndSet(false, true);
    // If no failure happens, let us see if there is new task coming.
    if (!isCurrentIterationFailed.get()) {
      if (topoClient.getNodeTopology(ite + 1) != null) {
        isNewTaskComing.set(true);
      }
    }
  }

  public synchronized void updateIteration() {
    if (isCurrentIterationFailed.get()) {
      iteration.set(nextIteration.get());
    } else {
      // If no failure, move to the next iteration
      iteration.incrementAndGet();
    }
    iteCommID.set(0);
    printLog("Update iteration to " + iteration.get());
    removeOldMsgInMap(iteration.get());
    // Reset
    isCurrentIterationFailed.set(false);
    nextIteration.set(0);
    isNewTaskComing.set(false);
    isIterationChecking.compareAndSet(true, false);
    // Update topology for the iteration
    updateNodeTopology(iteration.get());
    finishWorking();
  }

  public boolean isCurrentIterationFailed()
    throws UnsupportedOperationException {
    if (isIterationChecking.get()) {
      return isCurrentIterationFailed.get();
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public boolean isNewTaskComing() throws UnsupportedOperationException {
    if (isIterationChecking.get()) {
      return isNewTaskComing.get();
    } else {
      throw new UnsupportedOperationException();
    }
  }
}
