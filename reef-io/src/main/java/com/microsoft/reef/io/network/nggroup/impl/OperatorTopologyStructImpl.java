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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

  private final ConcurrentMap<String, Set<Integer>> deadMsgs = new ConcurrentHashMap<>();

  private int version;

  public OperatorTopologyStructImpl(final Class<? extends Name<String>> groupName,
      final Class<? extends Name<String>> operName, final String selfId, final String driverId,
      final Sender sender, final int version) {
    super();
    this.groupName = groupName;
    this.operName = operName;
    this.selfId = selfId;
    this.driverId = driverId;
    this.sender = sender;
    this.version = version;
  }

  public OperatorTopologyStructImpl(final OperatorTopologyStruct topology) {
    super();
    this.groupName = topology.getGroupName();
    this.operName = topology.getOperName();
    this.selfId = topology.getSelfId();
    this.driverId = topology.getDriverId();
    this.sender = topology.getSender();
    this.changes = topology.hasChanges();
    this.parent = topology.getParent();
    this.children.addAll(topology.getChildren());
  }

  @Override
  public NodeStruct getParent() {
    return parent;
  }

  @Override
  public Collection<? extends NodeStruct> getChildren() {
    return children;
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
    LOG.info(getQualifiedName() + "Adding " + msg.getType() + " into the data queue");
    final NodeStruct node = findNode(srcId);
    if(node==null) {
      LOG.warning("Unable to find node " + srcId + " to send " + msg.getType() + " to");
    } else {
      node.addData(msg);
      LOG.info(getQualifiedName() + "Added data msg to node " + srcId);
    }
  }

  /**
   * @param srcId
   * @return
   */
  private NodeStruct findNode(final String srcId) {
    if(parent!=null && parent.getId().equals(srcId)) {
      return parent;
    }
    return findChild(srcId);
  }

  @Override
  public void sendToParent(final byte[] data, final Type msgType) {
    if(parent==null) {
      LOG.warning(getQualifiedName() + "Perhaps parent has died or has not been configured");
      return;
    }
    final String parentId = parent.getId();
    try {
      LOG.info(getQualifiedName() + "Sending " + msgType + " msg to " + parentId);
      sender.send(Utils.bldVersionedGCM(groupName, operName, msgType, selfId,
          version, parentId, parent.getVersion(), data));
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending " + msgType + " data from " + selfId + " to " + parentId, e);
    }
  }

  @Override
  public void sendToChildren(final byte[] data, final Type msgType) {
    for (final NodeStruct childNode : children) {
      final String child = childNode.getId();
      try {
        LOG.info(getQualifiedName() + "Sending " + msgType + " msg to " + child);
        sender.send(Utils.bldVersionedGCM(groupName, operName, msgType, selfId,
            version, child, childNode.getVersion(), data));
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
    LOG.info(getQualifiedName() + "Waiting to receive from " + parent.getId());
    return parent.getData();
  }

  @Override
  public List<byte[]> recvFromChildren() {
    final List<byte[]> retLst = new ArrayList<byte[]>(children.size());

    for (final NodeStruct child : children) {
      LOG.info(getQualifiedName() + "Waiting to receive from child: " + child.getId());
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

  private boolean removedDeadMsg(final String msgSrcId, final int msgSrcVersion) {
    final Set<Integer> msgVersions = deadMsgs.get(msgSrcId);
    if(msgVersions!=null) {
      LOG.warning(getQualifiedName() + "Found dead msgs " +
          msgVersions + " waiting for add");
      if(msgVersions.remove(msgSrcVersion)) {
        LOG.warning(getQualifiedName()
            + "Found dead msg with same version as srcVer-" + msgSrcVersion);
        return true;
      }
      else {
        LOG.warning(getQualifiedName()
            + "No dead msg with same version as srcVer-"
            + msgSrcVersion);
      }
    }
    else {
      LOG.warning(getQualifiedName()
          + "No dead msgs waiting for add.");
    }
    return false;
  }

  private void addToDeadMsgs(final String srcId, final int version) {
    deadMsgs.putIfAbsent(srcId, new HashSet<Integer>());
    deadMsgs.get(srcId).add(version);
  }

  private boolean addedToDeadMsgs(final NodeStruct node, final String msgSrcId, final int msgSrcVersion) {
    if(node==null) {
      LOG.warning(getQualifiedName()
          + "Got dead msg when no node existed. OOS Queing up for add to handle");
      addToDeadMsgs(msgSrcId, msgSrcVersion);
      return true;
    }
    final int nodeVersion = node.getVersion();
    if(msgSrcVersion>nodeVersion) {
      LOG.warning(getQualifiedName() + "Got an OOS dead msg. " +
          "Has HIGHER ver-" + msgSrcVersion + " than node ver-" + nodeVersion +
          ". Queing up for add to handle");
      addToDeadMsgs(msgSrcId, msgSrcVersion);
      return true;
    }
    return false;
  }

  /**
   * Expects only control messages
   */
  @Override
  public void update(final GroupCommMessage msg) {
    if(!msg.hasSrcVersion()) {
      throw new RuntimeException(getQualifiedName()
          + "can only deal with msgs that have src version set");
    }
    final String srcId = msg.getSrcid();
    final int srcVersion = msg.getSrcVersion();
    LOG.info(getQualifiedName() + "Updating " + msg.getType() + " msg from " + srcId);
    LOG.info(getQualifiedName() + "Before update: parent=" + ((parent!=null) ? parent.getId() : "NULL"));
    LOG.info(getQualifiedName() + "Before update: children=" + children);
    switch(msg.getType()) {
    case ParentAdd:
      if(!removedDeadMsg(srcId, srcVersion)) {
        if(parent!=null) {
          LOG.warning(getQualifiedName() + "Parent already exists");
          final int parentVersion = parent.getVersion();
          if(srcVersion<parentVersion) {
            LOG.warning(getQualifiedName() + "Got an OOS parent add msg. " +
                "Has LOWER ver-" + srcVersion + " than parent ver-" + parentVersion +
                ". Discarding");
            break;
          }
          if(srcVersion>parentVersion) {
            LOG.warning(getQualifiedName() + "Got an OOS parent add msg. " +
                "Has HIGHER ver-" + srcVersion + " than parent ver-" + parentVersion
                + ". Bumping up version number");
            parent.setVersion(srcVersion);
            break;
          }
          else {
            final String logMsg = getQualifiedName() + "Got two parent add msgs of same version-" + srcVersion;
            LOG.warning(logMsg);
            throw new RuntimeException(logMsg);
          }
        }
        else {
          LOG.info(getQualifiedName() + "Creating new parent node for " + srcId);
          parent = new ParentNodeStruct(srcId,srcVersion);
        }
      }
      else {
        LOG.warning(getQualifiedName()
            + "Removed dead msg. Not adding parent");
      }
      break;
    case ParentDead:
      if(!addedToDeadMsgs(parent, srcId, srcVersion)) {
        final int parentVersion = parent.getVersion();
        if(srcVersion<parentVersion) {
          LOG.warning(getQualifiedName() + "Got an OOS parent dead msg. " +
              "Has LOWER ver-" + srcVersion + " than parent ver-" + parentVersion +
              ". Discarding");
          break;
        }
        else {
          LOG.info(getQualifiedName()
              + "Got a parent dead msg. " +
              "Has SAME ver-" + srcVersion + " as parent ver-" + parentVersion +
              "Setting parent node to null");
        }
      }
      else {
        LOG.warning(getQualifiedName()
            + "Added to dead msgs. Setting parent to null since ParentAdd might not turn up");
      }
      parent = null;
      break;
    case ChildAdd:
      if(!removedDeadMsg(srcId, srcVersion)) {
        final NodeStruct toBeAddedchild = findChild(srcId);
        if(toBeAddedchild!=null) {
          LOG.warning(getQualifiedName() + "Child already exists");
          final int childVersion = toBeAddedchild.getVersion();
          if(srcVersion<childVersion) {
            LOG.warning(getQualifiedName() + "Got an OOS child add msg. " +
                "Has LOWER ver-" + srcVersion + " than child ver-" + childVersion +
                ". Discarding");
            break;
          }
          if(srcVersion>childVersion) {
            LOG.warning(getQualifiedName() + "Got an OOS child add msg. " +
                "Has HIGHER ver-" + srcVersion + " than child ver-" + childVersion
                + ". Bumping up version number");
            toBeAddedchild.setVersion(srcVersion);
            break;
          }
          else {
            final String logMsg = getQualifiedName() + "Got two child add msgs of same version-" + srcVersion;
            LOG.warning(logMsg);
            throw new RuntimeException(logMsg);
          }
        }
        else {
          LOG.info(getQualifiedName() + "Creating new child node for " + srcId);
          children.add(new ChildNodeStruct(srcId, srcVersion));
        }
      }
      else {
        LOG.warning(getQualifiedName()
            + "Removed dead msg. Not adding child");
      }
      break;
    case ChildDead:
      final NodeStruct toBeRemovedchild = findChild(srcId);
      if(!addedToDeadMsgs(toBeRemovedchild, srcId, srcVersion)) {
        final int childVersion = toBeRemovedchild.getVersion();
        if(srcVersion<childVersion) {
          LOG.warning(getQualifiedName() + "Got an OOS child dead msg. " +
              "Has LOWER ver-" + srcVersion + " than child ver-" + childVersion +
              ". Discarding");
          break;
        }
        else {
          LOG.info(getQualifiedName()
              + "Got a child dead msg. " +
              "Has SAME ver-" + srcVersion + " as child ver-" + childVersion +
              "Removing child node");
        }
      }
      else {
        LOG.warning(getQualifiedName()
            + "Added to dead msgs. Removing child node since ChildAdd might not turn up");
      }
      children.remove(toBeRemovedchild);
      break;
    default:
      LOG.warning("Received a non control message in update");
      throw new RuntimeException("Received a non control message in update");
    }
    LOG.info(getQualifiedName() + "After update: parent=" + ((parent!=null) ? parent.getId() : "NULL"));
    LOG.info(getQualifiedName() + "After update: children=" + children);
  }


  /**
   * @param srcId
   * @return
   */
  private NodeStruct findChild(final String srcId) {
    for(final NodeStruct node : children) {
      if(node.getId().equals(srcId)) {
        return node;
      }
    }
    return null;
  }

  @Override
  public void update(final Set<GroupCommMessage> deletionDeltas) {
    LOG.info(getQualifiedName() + "Updating topology with deleting msgs");
    for (final GroupCommMessage delDelta : deletionDeltas) {
      update(delDelta);
    }
  }

  @Override
  public void setChanges(final boolean changes) {
    this.changes = changes;
  }

  /**
   * @return
   */
  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName) + ":"
        + selfId + ":ver(" + version + ") - ";
  }
}
