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

import javax.inject.Inject;

import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.group.operators.Broadcast;
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
public class BroadcastSender<T> implements Broadcast.Sender<T> {
  
  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String selfId;
  private final CommGroupNetworkHandler commGroupNetworkHandler;
  private final Codec<T> dataCodec;
  private final Set<String> childIds = new HashSet<>();
  private final NetworkService<GroupCommMessage> netService;
  private final BroadcastHandler handler;
  private final Sender sender;
  
  @Inject
  public BroadcastSender(
      @Parameter(CommunicationGroupName.class) Name<String> groupName,
      @Parameter(OperatorName.class) Name<String> operName,
      @Parameter(TaskConfigurationOptions.Identifier.class) String selfId,
      @Parameter(DataCodec.class) Codec<T> dataCodec,
      CommGroupNetworkHandler commGroupNetworkHandler,
      NetworkService<GroupCommMessage> netService) {
    super();
    this.groupName = (Class<? extends Name<String>>) groupName.getClass();
    this.operName = (Class<? extends Name<String>>) operName.getClass();
    this.selfId = selfId;
    this.dataCodec = dataCodec;
    this.commGroupNetworkHandler = commGroupNetworkHandler;
    this.netService = netService;
    this.handler = null;
    this.commGroupNetworkHandler.register(this.operName,handler);
    this.sender = new Sender(this.netService);
  }



  @Override
  public void send(T element) throws NetworkException,
      InterruptedException {
    for(String child : childIds){
      sender.send(Utils.bldGCM(groupName, operName, Type.Broadcast, selfId, child, dataCodec.encode(element)), child);
    }
  }

}
