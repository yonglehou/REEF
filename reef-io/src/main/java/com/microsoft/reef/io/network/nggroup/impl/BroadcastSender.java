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

import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.driver.parameters.DriverIdentifier;
import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.nggroup.api.CommGroupNetworkHandler;
import com.microsoft.reef.io.network.nggroup.api.OperatorTopology;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.CommunicationGroupName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.DataCodec;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.OperatorName;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;

/**
 *
 */
public class BroadcastSender<T> implements Broadcast.Sender<T>, EventHandler<GroupCommMessage> {

  private static final Logger LOG = Logger.getLogger(BroadcastSender.class
      .getName());


  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final CommGroupNetworkHandler commGroupNetworkHandler;
  private final Codec<T> dataCodec;
  private final NetworkService<GroupCommMessage> netService;
  private final Sender sender;

  private final OperatorTopology topology;

  @Inject
  public BroadcastSender(
      @Parameter(CommunicationGroupName.class) final String groupName,
      @Parameter(OperatorName.class) final String operName,
      @Parameter(TaskConfigurationOptions.Identifier.class) final String selfId,
      @Parameter(DataCodec.class) final Codec<T> dataCodec,
      @Parameter(DriverIdentifier.class) final String driverId,
      final CommGroupNetworkHandler commGroupNetworkHandler,
      final NetworkService<GroupCommMessage> netService) {
    super();
    LOG.info(operName + " has CommGroupHandler-" + commGroupNetworkHandler.toString());
    this.groupName = Utils.getClass(groupName);
    this.operName = Utils.getClass(operName);
    this.dataCodec = dataCodec;
    this.commGroupNetworkHandler = commGroupNetworkHandler;
    this.netService = netService;
    this.sender = new Sender(this.netService);
    this.topology = new OperatorTopologyImpl(this.groupName, this.operName, selfId, driverId, sender);
    this.commGroupNetworkHandler.register(this.operName,this);
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
    topology.handle(msg);
  }

  @Override
  public void send(final T element) throws NetworkException, InterruptedException {
    LOG.info("I am Broadcast sender root " + topology.getSelfId() + " for oper: " + operName + " in group " + groupName);
    topology.sendToChildren(dataCodec.encode(element),Type.Broadcast);
  }

}
