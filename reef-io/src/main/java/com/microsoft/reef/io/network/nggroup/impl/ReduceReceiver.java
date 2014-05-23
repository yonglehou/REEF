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
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.Identifier;

/**
 *
 */
public class ReduceReceiver<T> implements Reduce.Receiver<T> {
  private static final Logger LOG = Logger.getLogger(ReduceReceiver.class.getName());

  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String selfId;
  private final CommGroupNetworkHandler commGroupNetworkHandler;
  private final Codec<T> dataCodec;
  private final ReduceFunction<T> reduceFunction;
  private final String parent;
  private final Set<String> childIds = new HashSet<>();
  private final NetworkService<GroupCommMessage> netService;
  private final ReduceHandler handler;
  private final Sender sender;


  @Inject
  public ReduceReceiver(
      @Parameter(CommunicationGroupName.class) final String groupName,
      @Parameter(OperatorName.class) final String operName,
      @Parameter(TaskConfigurationOptions.Identifier.class) final String selfId,
      @Parameter(DataCodec.class) final Codec<T> dataCodec,
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
    this.handler = new ReduceHandlerImpl();
    this.commGroupNetworkHandler.register(this.operName,handler);
    this.parent = null;
    this.sender = new Sender(this.netService);
  }

  @Override
  public ReduceFunction<T> getReduceFunction() {
    return reduceFunction;
  }

  @Override
  public T reduce() throws InterruptedException, NetworkException {
    //I am root.
    LOG.log(Level.FINEST, "I am root " + selfId);
    //Wait for children to send
    final List<T> vals = new ArrayList<>(this.childIds.size());

    for (final String childIdentifier : this.childIds) {
      LOG.log(Level.FINEST, "Waiting for child: " + childIdentifier);
      final T cVal = dataCodec.decode(handler.get(childIdentifier));
      LOG.log(Level.FINEST, "Received: " + cVal);
      if (cVal != null) {
        vals.add(cVal);
      }
    }

    //Reduce the received values
    final T redVal = reduceFunction.apply(vals);
    LOG.log(Level.FINEST, "Local Reduced value: " + redVal);
    return redVal;
  }

  @Override
  public T reduce(final List<? extends Identifier> order)
      throws InterruptedException, NetworkException {
    throw new UnsupportedOperationException();
  }

}
