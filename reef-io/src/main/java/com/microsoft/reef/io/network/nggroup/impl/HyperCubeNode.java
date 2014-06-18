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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * This class describes a node in hyper cube. This class is modified from
 * TaskNodeImpl.
 * 
 * @author zhangbj
 */
public class HyperCubeNode {
  private static final Logger LOG = Logger.getLogger(HyperCubeNode.class
    .getName());

  private String taskID;
  private int nodeID;
  /** The list of send/receive operations on the neighbors */
  private final List<List<int[]>> neighborOpList;
  /** Status */
  private final AtomicBoolean isRunning;
  // The last iteration reported to the driver
  private int iteration = -1;

  public HyperCubeNode(String taskID, int nodeID) {
    this.taskID = taskID;
    this.nodeID = nodeID;
    this.neighborOpList = new ArrayList<List<int[]>>();
    this.isRunning = new AtomicBoolean(false);
  }

  public String getTaskID() {
    return this.taskID;
  }

  public int getNodeID() {
    return this.nodeID;
  }

  public List<List<int[]>> getNeighborOpList() {
    return neighborOpList;
  }

  public void setFailed() {
    this.isRunning.compareAndSet(true, false);
  }

  public void setRunning() {
    this.isRunning.compareAndSet(false, true);
  }

  public boolean isRunning() {
    return isRunning.get();
  }

  public void setIteration(int ite) {
    this.iteration = ite;
  }

  public int getIteration() {
    return iteration;
  }
  
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("{");
    sb.append(taskID);
    sb.append(",");
    sb.append(nodeID);
    sb.append(",");
    for (int i = 0; i < neighborOpList.size(); i++) {
      List<int[]> opDimList = neighborOpList.get(i);
      sb.append("[");
      for (int j = 0; j < opDimList.size(); j++) {
        sb.append("(");
        sb.append(+opDimList.get(j)[0]);
        sb.append(",");
        sb.append(opDimList.get(j)[1]);
        sb.append(")");
      }
      sb.append("]");
    }
    sb.append(",");
    sb.append(isRunning.get());
    sb.append(",");
    sb.append(iteration);
    sb.append("}");
    return sb.toString();
  }

  public void read(DataInput din) throws IOException {
    taskID = din.readUTF();
    nodeID = din.readInt();
    int opListSize = din.readInt();
    int opDimListSize = 0;
    int[] op = null;
    for (int i = 0; i < opListSize; i++) {
      opDimListSize = din.readInt();
      List<int[]> opDimList = new ArrayList<int[]>();
      for (int j = 0; j < opDimListSize; j++) {
        op = new int[] { din.readInt(), din.readInt() };
        opDimList.add(op);
      }
      neighborOpList.add(opDimList);
    }
    isRunning.set(din.readBoolean());
    iteration = din.readInt();
  }

  public void write(DataOutput dout) throws IOException {
    dout.writeUTF(taskID);
    dout.writeInt(nodeID);
    dout.writeInt(neighborOpList.size());
    for (int i = 0; i < neighborOpList.size(); i++) {
      List<int[]> opDimList = neighborOpList.get(i);
      dout.writeInt(opDimList.size());
      for (int j = 0; j < opDimList.size(); j++) {
        dout.writeInt(opDimList.get(j)[0]);
        dout.writeInt(opDimList.get(j)[1]);
      }
    }
    dout.writeBoolean(isRunning.get());
    dout.writeInt(iteration);
  }
}
