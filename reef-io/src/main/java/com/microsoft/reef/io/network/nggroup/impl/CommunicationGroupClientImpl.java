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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.GroupCommOperator;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.nggroup.api.CommGroupNetworkHandler;
import com.microsoft.reef.io.network.nggroup.api.GroupChanges;
import com.microsoft.reef.io.network.nggroup.api.GroupCommNetworkHandler;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.CommunicationGroupName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.OperatorName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.SerializedOperConfigs;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationSerializer;

/**
 *
 */
public class CommunicationGroupClientImpl implements com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient{
  private static final Logger LOG = Logger.getLogger(CommunicationGroupClientImpl.class.getName());

  private final GroupCommNetworkHandler groupCommNetworkHandler;
  private final Class<? extends Name<String>> groupName;
  private final Map<Class<? extends Name<String>>, GroupCommOperator> operators;

  @Inject
  public CommunicationGroupClientImpl(
        @Parameter(CommunicationGroupName.class) final String groupName,
        @Parameter(TaskConfigurationOptions.Identifier.class) final String taskId,
        final GroupCommNetworkHandler groupCommNetworkHandler,
        @Parameter(SerializedOperConfigs.class) final Set<String> operatorConfigs,
        final ConfigurationSerializer configSerializer,
        final NetworkService<GroupCommMessage> netService
      ){
    LOG.info(groupName + " has GroupCommHandler-"
        + groupCommNetworkHandler.toString());
    this.groupName = Utils.getClass(groupName);
    this.groupCommNetworkHandler = groupCommNetworkHandler;
    this.operators = new HashMap<>();
    try {
      final CommGroupNetworkHandler commGroupNetworkHandler = Tang.Factory.getTang().newInjector().getInstance(CommGroupNetworkHandler.class);
      this.groupCommNetworkHandler.register(this.groupName, commGroupNetworkHandler);

      for (final String operatorConfigStr : operatorConfigs) {

        final Configuration operatorConfig = configSerializer.fromString(operatorConfigStr);
        final Injector injector = Tang.Factory.getTang().newInjector(operatorConfig);

        injector.bindVolatileParameter(CommunicationGroupName.class, groupName);
        injector.bindVolatileInstance(CommGroupNetworkHandler.class, commGroupNetworkHandler);
        injector.bindVolatileInstance(NetworkService.class, netService);

        final GroupCommOperator operator = injector.getInstance(GroupCommOperator.class);
        final String operName = injector.getNamedInstance(OperatorName.class);
        this.operators.put(Utils.getClass(operName), operator);
        LOG.info(operName + " has CommGroupHandler-" + commGroupNetworkHandler.toString());
      }
    } catch (BindException | IOException e) {
      throw new RuntimeException("Unable to deserialize operator config", e);
    } catch (final InjectionException e) {
      throw new RuntimeException("Unable to deserialize operator config", e);
    }
  }

  @Override
  public Broadcast.Sender getBroadcastSender(final Class<? extends Name<String>> operatorName) {
    final GroupCommOperator op = operators.get(operatorName);
    if(!(op instanceof Broadcast.Sender)) {
      throw new RuntimeException("Configured operator is not a broadcast sender");
    }
    return (Broadcast.Sender)op;
  }

  @Override
  public Reduce.Receiver getReduceReceiver(final Class<? extends Name<String>> operatorName) {
    final GroupCommOperator op = operators.get(operatorName);
    if(!(op instanceof Reduce.Receiver)) {
      throw new RuntimeException("Configured operator is not a reduce receiver");
    }
    return (Reduce.Receiver)op;
  }

  @Override
  public Broadcast.Receiver getBroadcastReceiver(
      final Class<? extends Name<String>> operatorName) {
    final GroupCommOperator op = operators.get(operatorName);
    if(!(op instanceof Broadcast.Receiver)) {
      throw new RuntimeException("Configured operator is not a broadcast receiver");
    }
    return (Broadcast.Receiver)op;
  }

  @Override
  public Reduce.Sender getReduceSender(
      final Class<? extends Name<String>> operatorName) {
    final GroupCommOperator op = operators.get(operatorName);
    if(!(op instanceof Reduce.Sender)) {
      throw new RuntimeException("Configured operator is not a reduce sender");
    }
    return (Reduce.Sender)op;
  }

  @Override
  public GroupChanges synchronize() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void waitFor(final int numberOfReceivers, final int timeout, final TimeUnit unit)
      throws TimeoutException {
    // TODO Auto-generated method stub

  }

  @Override
  public void waitFor(final int timeout, final TimeUnit unit) throws TimeoutException {
    // TODO Auto-generated method stub

  }

  @Override
  public Class<? extends Name<String>> getName() {
    return groupName;
  }
}
