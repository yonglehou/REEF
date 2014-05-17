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

import javax.inject.Inject;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.GroupCommOperator;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.CommGroupNetworkHandler;
import com.microsoft.reef.io.network.nggroup.api.GroupChanges;
import com.microsoft.reef.io.network.nggroup.api.GroupCommNetworkHandler;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.CommunicationGroupName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.OperatorName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.SerializedOperConfigs;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationSerializer;

/**
 * 
 */
public class CommunicationGroupClient implements com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient{
  
  private final GroupCommNetworkHandler groupCommNetworkHandler;
  private final Class<? extends Name<String>> groupName;
  private final Map<Class<? extends Name<String>>, GroupCommOperator> operators;
  
  @Inject
  public CommunicationGroupClient(
        @Parameter(CommunicationGroupName.class) Class<? extends Name<String>> groupName,
        GroupCommNetworkHandler groupCommNetworkHandler,
        @Parameter(SerializedOperConfigs.class) Set<String> operatorConfigs,
        ConfigurationSerializer configSerializer,
        Injector injector
      ){
    this.groupName = groupName;
    this.groupCommNetworkHandler = groupCommNetworkHandler;
    this.operators = new HashMap<>();
    for (String operatorConfigStr : operatorConfigs) {
      try {
        /**
         * When the operator is instantiated it needs
         * to get this commGroupNetworkHandler injected
         * and register its handler with commGroupNetworkHandler
         * for its name
         */
        CommGroupNetworkHandler commGroupNetworkHandler = injector.getInstance(CommGroupNetworkHandler.class);
        this.groupCommNetworkHandler.register(groupName, commGroupNetworkHandler);
        Configuration operatorConfig = configSerializer.fromString(operatorConfigStr);
        Injector forkedInjector = injector.forkInjector(operatorConfig);
        GroupCommOperator operator = forkedInjector.getInstance(GroupCommOperator.class);
        Name<String> operName = injector.getNamedInstance(OperatorName.class);
        this.operators.put((Class<? extends Name<String>>) operName.getClass(), operator);
      } catch (BindException | IOException e) {
        throw new RuntimeException("Unable to deserialize operator config", e);
      } catch (InjectionException e) {
        throw new RuntimeException("Unable to deserialize operator config", e);
      }
    }
  }

  @Override
  public Broadcast.Sender getBroadcastSender(Class<? extends Name<String>> operatorName) {
    GroupCommOperator op = operators.get(operatorName);
    if(!(op instanceof Broadcast.Sender))
      throw new RuntimeException("Configured operator is not a broadcast sender");
    return (Broadcast.Sender)op;  
  }

  @Override
  public Reduce.Receiver getReduceReceiver(Class<? extends Name<String>> operatorName) {
    GroupCommOperator op = operators.get(operatorName);
    if(!(op instanceof Reduce.Receiver))
      throw new RuntimeException("Configured operator is not a reduce receiver");
    return (Reduce.Receiver)op;
  }

  @Override
  public Broadcast.Receiver getBroadcastReceiver(
      Class<? extends Name<String>> operatorName) {
    GroupCommOperator op = operators.get(operatorName);
    if(!(op instanceof Broadcast.Receiver))
      throw new RuntimeException("Configured operator is not a broadcast receiver");
    return (Broadcast.Receiver)op; 
  }

  @Override
  public Reduce.Sender getReduceSender(
      Class<? extends Name<String>> operatorName) {
    GroupCommOperator op = operators.get(operatorName);
    if(!(op instanceof Reduce.Sender))
      throw new RuntimeException("Configured operator is not a reduce sender");
    return (Reduce.Sender)op; 
  }

  @Override
  public GroupChanges synchronize() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void waitFor(int numberOfReceivers, int timeout, TimeUnit unit)
      throws TimeoutException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void waitFor(int timeout, TimeUnit unit) throws TimeoutException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Class<? extends Name<String>> getName() {
    return groupName;
  }

}
