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

/**
 * This class describes a node in hyper cube. This class is modified from
 * TaskNodeImpl.
 * 
 * @author zhangbj
 */
public class HyperCubeNode {

  private String taskID;
  private int nodeID;
  /** The list of send/receive operations on the neighbors */
  private final List<List<int[]>> neighborOpList = new ArrayList<List<int[]>>();

  public HyperCubeNode(String taskID, int nodeID) {
    this.taskID = taskID;
    this.nodeID = nodeID;
  }

  public String getTaskID() {
    return taskID;
  }

  public int getNodeID() {
    return nodeID;
  }

  public List<List<int[]>> getNeighborOpList() {
    return neighborOpList;
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
    sb.append("}");
    return sb.toString();
  }
  
  public HyperCubeNode clone() {
    HyperCubeNode node = new HyperCubeNode(taskID, nodeID);
    int[] op = null;
    for (int i = 0; i < neighborOpList.size(); i++) {
      List<int[]> opDimList = new ArrayList<int[]>();
      for (int j = 0; j < neighborOpList.get(i).size(); j++) {
        op =
          new int[] { neighborOpList.get(i).get(j)[0],
            neighborOpList.get(i).get(j)[1] };
        opDimList.add(op);
      }
      node.getNeighborOpList().add(opDimList);
    }
    return node;
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
  }
}
