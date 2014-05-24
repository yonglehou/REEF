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

import java.util.Set;
import java.util.logging.Logger;

import com.microsoft.reef.io.network.group.operators.GroupCommOperator;
import com.microsoft.reef.io.network.nggroup.api.NeighborStatus;
import com.microsoft.reef.io.network.nggroup.api.OperatorHandler;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;

/**
 *
 */
public class TopologyUpdateHelper {

  private static final Logger LOG = Logger.getLogger(TopologyUpdateHelper.class.getName());

  public static void updateTopology(final GroupCommOperator operator, final Set<String> childIds) {
    LOG.info("Updating topology for operator: " + operator.getOperName() + " in group " + operator.getGroupName());
    final OperatorHandler handler = operator.getHandler();
    final NeighborStatus status = handler.updateTopology();
    for(final String neighbor : status) {
      final Type t = status.getStatus(neighbor);
      if(t!=null) {
        switch(t) {
        case ChildAdd:
          LOG.info("Adding child " + neighbor + " to " + operator.getOperName());
          childIds.add(neighbor);
          break;

        case ChildDead:
          LOG.info("Removing child " + neighbor + " from " + operator.getOperName());
          childIds.remove(neighbor);
          break;

        case ParentAdd:
          LOG.info("Adding parent " + neighbor + " to " + operator.getOperName());
          operator.setParent(neighbor);
          break;

        case ParentDead:
          LOG.info("Removing parent " + neighbor + " from " + operator.getOperName());
          operator.setParent(null);
          break;

        default:
            LOG.warning("Received an unexpected status message " + t.toString() + " for " + neighbor);
            break;
        }
      }
      else {
        LOG.warning("No change in status for " + neighbor);
      }
    }
    status.updateProcessed();
  }
}
