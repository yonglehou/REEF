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

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.group.operators.Broadcast.Receiver;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.nggroup.api.BroadcastHandler;
import com.microsoft.reef.io.network.nggroup.api.CommGroupNetworkHandler;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.CommunicationGroupName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.DataCodec;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.OperatorName;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.Parameter;

/**
 *
 */
public class BroadcastReceiver<T> implements Receiver<T> {

  private static final Logger LOG = Logger.getLogger(BroadcastReceiver.class.getName());

  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String selfId;
  private final CommGroupNetworkHandler commGroupNetworkHandler;
  private final Codec<T> dataCodec;
  private final String parent;
  private final Set<String> childIds = new HashSet<>();
  private final NetworkService<GroupCommMessage> netService;
  private final BroadcastHandler handler;
  private final Sender sender;


  @Inject
  public BroadcastReceiver(
      @Parameter(CommunicationGroupName.class) final String groupName,
      @Parameter(OperatorName.class) final String operName,
      @Parameter(TaskConfigurationOptions.Identifier.class) final String selfId,
      @Parameter(DataCodec.class) final Codec<T> dataCodec,
      final CommGroupNetworkHandler commGroupNetworkHandler,
      final NetworkService<GroupCommMessage> netService) {
    super();
    LOG.info(operName + " has CommGroupHandler-"
        + commGroupNetworkHandler.toString());
    this.groupName = Utils.getClass(groupName);
    this.operName = Utils.getClass(operName);
    this.selfId = selfId;
    this.dataCodec = dataCodec;
    this.commGroupNetworkHandler = commGroupNetworkHandler;
    this.netService = netService;
    this.handler = new BroadcastHandlerImpl();
    this.commGroupNetworkHandler.register(this.operName,handler);
    this.parent = null;
    this.sender = new Sender(this.netService);
  }


  @Override
  public T receive() throws NetworkException, InterruptedException {
  //I am an intermediate node or leaf.
    final T retVal;
    if (this.parent != null) {
      //Wait for parent to send
      LOG.log(Level.FINEST, "Waiting for parent: " + parent);
      retVal = dataCodec.decode(handler.get(parent));
      LOG.log(Level.FINEST, "Received: " + retVal);

      LOG.log(Level.FINEST, "Sending " + retVal + " to " + childIds);
      for (final String child : childIds) {
        LOG.log(Level.FINEST, "Sending " + retVal + " to child: " + child);
        sender.send(Utils.bldGCM(groupName, operName, Type.Broadcast, selfId, child, dataCodec.encode(retVal)), child);
      }
    } else {
      retVal = null;
    }
    return retVal;
  }

}
