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
package com.microsoft.reef.runtime.yarn.driver;

import org.apache.hadoop.yarn.client.api.AMRMClient;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Side-channel to request containers from YARN using AMRMClient.ContainerRequest requests.
 */
public final class YarnContainerRequestHandler {
  private static final Logger LOG = Logger.getLogger(YarnContainerRequestHandler.class.getName());

  private final YarnContainerManager containerManager;


  @Inject
  YarnContainerRequestHandler(final YarnContainerManager containerManager) {
    this.containerManager = containerManager;
    LOG.log(Level.FINEST, "Instantiated 'YarnContainerRequestHandler'");
  }

  /**
   * Enqueue a set of container requests with YARN.
   *
   * @param containerRequests
   */
  public void onContainerRequest(final AMRMClient.ContainerRequest... containerRequests) {
    LOG.log(Level.FINEST, "Sending container requests to YarnContainerManager.");
    this.containerManager.onContainerRequest(containerRequests);
  }

}
