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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.nggroup.api.CommGroupNetworkHandler;
import com.microsoft.reef.io.network.nggroup.api.ReduceHandler;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.CommunicationGroupName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.DataCodec;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.OperatorName;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.Parameter;

/**
 * 
 */
public class ReduceSender<T> implements Reduce.Sender<T> {
 private static final Logger LOG = Logger.getLogger(ReduceSender.class.getName());
  
  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String selfId;
  private final CommGroupNetworkHandler commGroupNetworkHandler;
  private final Codec<T> dataCodec;
  private final ReduceFunction<T> reduceFunction;
  private String parent;
  private final Set<String> childIds = new HashSet<>();
  private final NetworkService<GroupCommMessage> netService;
  private final ReduceHandler handler;
  private final Sender sender;
  
  
  @Inject
  public ReduceSender(
      @Parameter(CommunicationGroupName.class) Name<String> groupName,
      @Parameter(OperatorName.class) Name<String> operName,
      @Parameter(TaskConfigurationOptions.Identifier.class) String selfId,
      @Parameter(DataCodec.class) Codec<T> dataCodec,
      @Parameter(com.microsoft.reef.io.network.nggroup.impl.config.parameters.ReduceFunction.class) ReduceFunction<T> reduceFunction,
      CommGroupNetworkHandler commGroupNetworkHandler,
      NetworkService<GroupCommMessage> netService) {
    super();
    this.groupName = (Class<? extends Name<String>>) groupName.getClass();
    this.operName = (Class<? extends Name<String>>) operName.getClass();
    this.selfId = selfId;
    this.dataCodec = dataCodec;
    this.reduceFunction = reduceFunction;
    this.commGroupNetworkHandler = commGroupNetworkHandler;
    this.netService = netService;
    this.handler = null;
    this.commGroupNetworkHandler.register(this.operName,handler);
    this.parent = null;
    this.sender = new Sender(this.netService);
  }

  @Override
  public void send(T myData) throws NetworkException,
      InterruptedException {
    LOG.log(Level.FINEST, "I am Reduce sender" + selfId);
    final List<T> vals = new ArrayList<>(this.childIds.size() + 1);
    vals.add(myData);
    for (final String childId : childIds) {
      LOG.log(Level.FINEST, "Waiting for child: " + childId);
      final Optional<T> valueFromChild = getValueForChild(childId);
      if (valueFromChild.isPresent()) {
        vals.add(valueFromChild.get());
      }
    }

    //Reduce the received values
    final T reducedValue = reduceFunction.apply(vals);
    LOG.log(Level.FINEST, "Sending " + reducedValue + " to parent: " + parent);
    assert (parent != null);
    sender.send(Utils.bldGCM(groupName, operName, Type.Reduce, selfId, parent, dataCodec.encode(reducedValue)), parent);
  }

  /**
   * @param childId
   * @return
   */
  private Optional<T> getValueForChild(String childId) {
    LOG.log(Level.FINEST, "Waiting for child: " + childId);
    final T valueFromChild = dataCodec.decode(handler.get(childId));
    LOG.log(Level.FINEST, "Received: " + valueFromChild);
    final Optional<T> returnValue;
    if (valueFromChild != null) {
      returnValue = Optional.of(valueFromChild);
    }
    else{
      returnValue = Optional.empty();
    }
    return returnValue;
  }

  @Override
  public ReduceFunction<T> getReduceFunction() {
    return reduceFunction;
  }

}
