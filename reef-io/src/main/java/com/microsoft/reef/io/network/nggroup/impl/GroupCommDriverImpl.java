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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.context.ServiceConfiguration;
import com.microsoft.reef.driver.parameters.DriverIdentifier;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.Connection;
import com.microsoft.reef.io.network.Message;
import com.microsoft.reef.io.network.group.impl.GCMCodec;
import com.microsoft.reef.io.network.impl.BindNSToTask;
import com.microsoft.reef.io.network.impl.MessagingTransportFactory;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.impl.NetworkServiceClosingHandler;
import com.microsoft.reef.io.network.impl.NetworkServiceParameters;
import com.microsoft.reef.io.network.impl.UnbindNSFromTask;
import com.microsoft.reef.io.network.naming.NameServer;
import com.microsoft.reef.io.network.naming.NameServerParameters;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupDriver;
import com.microsoft.reef.io.network.nggroup.api.GroupCommDriver;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.SerializedGroupConfigs;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.formats.ConfigurationSerializer;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.impl.LoggingEventHandler;
import com.microsoft.wake.impl.ThreadPoolStage;
import com.microsoft.wake.remote.NetUtils;

/**
 *
 */
public class GroupCommDriverImpl implements GroupCommDriver {
  private static final Logger LOG = Logger.getLogger(GroupCommDriverImpl.class.getName());
  /**
   * TANG instance
   */
  private static final Tang tang = Tang.Factory.getTang();

  private final AtomicInteger contextIds = new AtomicInteger(0);

  private final IdentifierFactory idFac = new StringIdentifierFactory();

  private final NameServer nameService = new NameServer(0, idFac);

  private final String nameServiceAddr;
  private final int nameServicePort;

  private final Set<CommunicationGroupDriver> commGroupDrivers = new HashSet<>();

  private final ConfigurationSerializer confSerializer;

  private final NetworkService<GroupCommMessage> netService;

  private final EStage<GroupCommMessage> senderStage;

  @Inject
  public GroupCommDriverImpl(final ConfigurationSerializer confSerializer,
      @Parameter(DriverIdentifier.class) final String driverId){
    this.nameServiceAddr = NetUtils.getLocalAddress();
    this.nameServicePort = nameService.getPort();
    this.confSerializer = confSerializer;
    this.netService = new NetworkService<>(idFac, 0, nameServiceAddr,
        nameServicePort, new GCMCodec(),
        new MessagingTransportFactory(),
        new LoggingEventHandler<Message<GroupCommMessage>>(),
        new LoggingEventHandler<Exception>());
    this.netService.registerId(idFac.getNewInstance(driverId));
    this.senderStage = new ThreadPoolStage<>(
        "SrcCtrlMsgSender", new EventHandler<GroupCommMessage>() {
      @Override
      public void onNext(final GroupCommMessage srcCtrlMsg) {

        final Identifier id = GroupCommDriverImpl.this.idFac.getNewInstance(srcCtrlMsg.getDestid());

        final Connection<GroupCommMessage> link = GroupCommDriverImpl.this.netService.newConnection(id);
        try {
          link.open();
          LOG.log(Level.FINEST, "Sending source ctrl msg {0} for {1} to {2}",
              new Object[] { srcCtrlMsg.getType(), srcCtrlMsg.getSrcid(), id });
          link.write(srcCtrlMsg);
        } catch (final NetworkException e) {
          LOG.log(Level.WARNING, "Unable to send ctrl task msg to parent " + id, e);
          throw new RuntimeException("Unable to send ctrl task msg to parent " + id, e);
        }
      }
    }, 5);
  }

  @Override
  public CommunicationGroupDriver newCommunicationGroup(
      final Class<? extends Name<String>> groupName) {
    final CommunicationGroupDriver commGroupDriver = new CommunicationGroupDriverImpl(groupName, confSerializer, senderStage);
    commGroupDrivers.add(commGroupDriver);
    return commGroupDriver;
  }

  @Override
  public boolean configured(final ActiveContext activeContext) {
    return activeContext.getId().startsWith("GroupCommunicationContext-");
  }

  @Override
  public Configuration getContextConf() {
    return ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, "GroupCommunicationContext-" + contextIds.getAndIncrement())
        .build();
  }

  @Override
  public Configuration getServiceConf() {
    final Configuration serviceConfiguration = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, NetworkService.class)
        .set(ServiceConfiguration.SERVICES, GroupCommNetworkHandlerImpl.class)
        .set(ServiceConfiguration.ON_CONTEXT_STOP,NetworkServiceClosingHandler.class)
        .set(ServiceConfiguration.ON_TASK_STARTED, BindNSToTask.class)
        .set(ServiceConfiguration.ON_TASK_STOP, UnbindNSFromTask.class)
        .build();
    return tang.newConfigurationBuilder(serviceConfiguration)
      .bindNamedParameter(NetworkServiceParameters.NetworkServiceCodec.class, GCMCodec.class)
      .bindNamedParameter(NetworkServiceParameters.NetworkServiceHandler.class, GroupCommNetworkHandlerImpl.class)
      .bindNamedParameter(NetworkServiceParameters.NetworkServiceExceptionHandler.class, ExceptionHandler.class)
      .bindNamedParameter(NameServerParameters.NameServerAddr.class, nameServiceAddr)
      .bindNamedParameter(NameServerParameters.NameServerPort.class, Integer.toString(nameServicePort))
      .bindNamedParameter(NetworkServiceParameters.NetworkServicePort.class, "0")
      .build();
  }

  @Override
  public Configuration getTaskConfiguration(final Configuration partialTaskConf) {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder(partialTaskConf);
    for (final CommunicationGroupDriver commGroupDriver : commGroupDrivers) {
      final Configuration commGroupConf = commGroupDriver.getConfiguration(partialTaskConf);
      jcb.bindSetEntry(SerializedGroupConfigs.class, confSerializer.toString(commGroupConf));
    }
    return jcb.build();
  }

  @Override
  public void handle(final RunningTask runningTask) {
    for (final CommunicationGroupDriver commGroupDriver : commGroupDrivers) {
      commGroupDriver.handle(runningTask);
    }
  }

  @Override
  public void handle(final FailedTask failedTask) {
    for (final CommunicationGroupDriver commGroupDriver : commGroupDrivers) {
      commGroupDriver.handle(failedTask);
    }
  }



}
