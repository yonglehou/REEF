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
package com.microsoft.reef.driver.client;

import com.microsoft.reef.annotations.Optional;
import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;

/**
 * The driver uses this interface to communicate with the job client.
 * <p/>
 * Note that as of REEF 0.4, the presence of a client is no longer guaranteed, depending on the deployment environment.
 */
@Public
@DriverSide
@Provided
@Optional
public interface JobMessageObserver {

  /**
   * Send a message to the client.
   *
   * @param message a message to be sent to the client
   */
  public void sendMessageToClient(final byte[] message);

}
