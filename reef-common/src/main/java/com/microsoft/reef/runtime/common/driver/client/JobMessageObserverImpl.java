/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.common.driver.client;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.client.JobMessageObserver;

import javax.inject.Inject;

/**
 * An implementation of JobMessageObserver.
 */
@DriverSide
public final class JobMessageObserverImpl implements JobMessageObserver {

  private final ClientConnection clientConnection;

  @Inject
  public JobMessageObserverImpl(final ClientConnection clientConnection) {
    this.clientConnection = clientConnection;
  }

  @Override
  public void sendMessageToClient(final byte[] message) {
    this.clientConnection.sendMessage(message);
  }

}
