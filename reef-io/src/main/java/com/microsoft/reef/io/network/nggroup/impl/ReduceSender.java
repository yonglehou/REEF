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
import com.microsoft.reef.io.network.nggroup.api.OperatorHandler;
import com.microsoft.reef.io.network.nggroup.api.ReduceHandler;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.CommunicationGroupName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.DataCodec;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.NumberOfReceivers;
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
      @Parameter(CommunicationGroupName.class) final String groupName,
      @Parameter(OperatorName.class) final String operName,
      @Parameter(TaskConfigurationOptions.Identifier.class) final String selfId,
      @Parameter(DataCodec.class) final Codec<T> dataCodec,
      @Parameter(NumberOfReceivers.class) final int numberOfReceivers,
      @Parameter(com.microsoft.reef.io.network.nggroup.impl.config.parameters.ReduceFunctionParam.class) final ReduceFunction<T> reduceFunction,
      final CommGroupNetworkHandler commGroupNetworkHandler,
      final NetworkService<GroupCommMessage> netService) {
    super();
    LOG.info(operName + " has CommGroupHandler-"
        + commGroupNetworkHandler.toString());
    this.groupName = Utils.getClass(groupName);
    this.operName = Utils.getClass(operName);
    this.selfId = selfId;
    this.dataCodec = dataCodec;
    this.reduceFunction = reduceFunction;
    this.commGroupNetworkHandler = commGroupNetworkHandler;
    this.netService = netService;
    this.handler = new ReduceHandlerImpl(1,0);
    this.commGroupNetworkHandler.register(this.operName,handler);
    this.parent = null;
    this.sender = new Sender(this.netService);
  }


  @Override
  public void updateTopology() {
    TopologyUpdateHelper.updateTopology(this, childIds);
  }

  @Override
  public void waitForSetup() {
    handler.waitForSetup();
    updateTopology();
  }

  /**
   * @param parent the parent to set
   */
  @Override
  public void setParent(final String parent) {
    this.parent = parent;
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
  public OperatorHandler getHandler() {
    return handler;
  }

  @Override
  public void send(final T myData) throws NetworkException,
      InterruptedException {
    LOG.log(Level.INFO, "I am Reduce sender" + selfId);
    final List<T> vals = new ArrayList<>(this.childIds.size() + 1);
    vals.add(myData);
    for (final String childId : childIds) {
      LOG.log(Level.INFO, "Waiting for child: " + childId);
      final Optional<T> valueFromChild = getValueForChild(childId);
      if (valueFromChild.isPresent()) {
        vals.add(valueFromChild.get());
      }
    }

    //Reduce the received values
    final T reducedValue = reduceFunction.apply(vals);
    LOG.log(Level.INFO, "Sending " + reducedValue + " to parent: " + parent);
    assert (parent != null);
    sender.send(Utils.bldGCM(groupName, operName, Type.Reduce, selfId, parent, dataCodec.encode(reducedValue)), parent);
  }

  /**
   * @param childId
   * @return
   * @throws InterruptedException
   */
  private Optional<T> getValueForChild(final String childId)
      throws InterruptedException {
    LOG.log(Level.INFO, "Waiting for child: " + childId);
    final T valueFromChild = dataCodec.decode(handler.get(childId));
    LOG.log(Level.INFO, "Received: " + valueFromChild);
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
