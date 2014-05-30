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
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.nggroup.api.NodeStruct;
import com.microsoft.reef.io.network.nggroup.api.OperatorTopologyStruct;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.tang.annotations.Name;

/**
 *
 */
public class OperatorTopologyStructImpl implements OperatorTopologyStruct {

  private static final Logger LOG = Logger.getLogger(OperatorTopologyStructImpl.class.getName());

  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String selfId;
  private final String driverId;
  private final Sender sender;

  private boolean changes = true;
  private NodeStruct parent;
  private final List<NodeStruct> children = new ArrayList<>();

  public OperatorTopologyStructImpl(final Class<? extends Name<String>> groupName,
      final Class<? extends Name<String>> operName, final String selfId, final String driverId,
      final Sender sender) {
    super();
    this.groupName = groupName;
    this.operName = operName;
    this.selfId = selfId;
    this.driverId = driverId;
    this.sender = sender;
  }

  public OperatorTopologyStructImpl(final OperatorTopologyStruct topology) {
    super();
    this.groupName = topology.getGroupName();
    this.operName = topology.getOperName();
    this.selfId = topology.getSelfId();
    this.driverId = topology.getDriverId();
    this.sender = topology.getSender();
  }



  @Override
  public Class<? extends Name<String>> getGroupName() {
    return groupName;
  }

  @Override
  public Class<? extends Name<String>> getOperName() {
    return operName;
  }

  @Override
  public String getSelfId() {
    return selfId;
  }

  @Override
  public String getDriverId() {
    return driverId;
  }

  @Override
  public Sender getSender() {
    return sender;
  }

  @Override
  public boolean hasChanges() {
    return this.changes;
  }

  @Override
  public void addAsData(final GroupCommMessage msg) {
    final String srcId = msg.getSrcid();
    LOG.info("Adding " + msg.getType() + " into the data queue of " + srcId);
    final NodeStruct node = findNode(srcId);
    if(node==null) {
      LOG.warning("Unable to find node " + srcId + " to send " + msg.getType() + " to");
    } else {
      node.addData(msg);
    }
  }

  /**
   * @param srcId
   * @return
   */
  private NodeStruct findNode(final String srcId) {
    if(parent.getId()==srcId) {
      return parent;
    }
    return findChild(srcId);
  }

  @Override
  public void sendToParent(final byte[] data, final Type msgType) {
    if(parent==null) {
      LOG.warning("Perhaps parent has died or has not been configured");
      return;
    }
    final String parentId = parent.getId();
    try {
      sender.send(Utils.bldGCM(groupName, operName, msgType, selfId, parentId, data));
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending " + msgType + " data from " + selfId + " to " + parentId, e);
    }
  }

  @Override
  public void sendToChildren(final byte[] data, final Type msgType) {
    for (final NodeStruct childNode : children) {
      final String child = childNode.getId();
      try {
        sender.send(
            Utils.bldGCM(groupName, operName, msgType, selfId, child, data));
      } catch (final NetworkException e) {
        throw new RuntimeException("NetworkException while sending "
            + msgType + " data from " + selfId + " to " + child, e);
      }
    }
  }

  @Override
  public byte[] recvFromParent() {
    if(parent==null) {
      LOG.warning("Perhaps parent has died or has not been configured");
      return null;
    }
    return parent.getData();
  }

  @Override
  public List<byte[]> recvFromChildren() {
    final List<byte[]> retLst = new ArrayList<byte[]>(children.size());

    for (final NodeStruct child : children) {
      LOG.log(Level.INFO, "Waiting for child: " + child.getId());
      final byte[] retVal = child.getData();
      if(retVal!=null) {
        retLst.add(retVal);
      }
      else {
        LOG.warning("Child " + child.getId() + " has died");
      }
    }
    return retLst;
  }

  /**
   * Expects only control messages
   */
  @Override
  public void update(final GroupCommMessage msg) {
    final String srcId = msg.getSrcid();
    LOG.info("Updating " + msg.getType() + " msg from " + srcId);
    switch(msg.getType()) {
    case ParentAdd:
      parent = new ParentNodeStruct(srcId);
      break;
    case ParentDead:
      parent = null;
      break;
    case ChildAdd:
      children.add(new ChildNodeStruct(srcId));
      break;
    case ChildDead:
      final NodeStruct child = findChild(srcId);
      if(child!=null) {
        children.remove(child);
      } else {
        LOG.warning("Received a ChildDead message for non-existent child " + srcId);
      }
      break;
    default:
      LOG.warning("Received a non control message in update");
      throw new RuntimeException("Received a non control message in update");
    }
  }

  /**
   * @param srcId
   * @return
   */
  private NodeStruct findChild(final String srcId) {
    for(final NodeStruct node : children) {
      if(node.getId()==srcId) {
        return node;
      }
    }
    return null;
  }

  @Override
  public void update(final Set<GroupCommMessage> deletionDeltas) {
    for (final GroupCommMessage delDelta : deletionDeltas) {
      update(delDelta);
    }
  }

  @Override
  public void setChanges(final boolean changes) {
    this.changes = changes;
  }
}
