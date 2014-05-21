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

import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.Connection;
import com.microsoft.reef.io.network.group.operators.GroupCommOperator;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;

/**
 * 
 */
public class Sender implements GroupCommOperator{
  private final NetworkService<GroupCommMessage> netService;
  private final IdentifierFactory idFac = new StringIdentifierFactory();
  
  public Sender(NetworkService<GroupCommMessage> netService){
    this.netService = netService;
  }
  
  public void send(final GroupCommMessage msg, final String child) throws NetworkException{
    Identifier childId = idFac.getNewInstance(child);
    final Connection<GroupCommMessage> link = netService.newConnection(childId);
    link.open();
    link.write(msg);
  }
}
