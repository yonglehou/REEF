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
package com.microsoft.reef.runtime.common.client;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.RemoteMessage;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Handler for JobStatus messages from running jobs
 */
@ClientSide
@Private
final class JobStatusMessageHandler implements EventHandler<RemoteMessage<ReefServiceProtos.JobStatusProto>> {
  private final Logger LOG = Logger.getLogger(JobStatusMessageHandler.class.getName());
  private final RunningJobs runningJobs;

  @Inject
  JobStatusMessageHandler(final RunningJobs runningJobs) {
    this.runningJobs = runningJobs;
    LOG.log(Level.INFO, "Instantiated 'JobStatusMessageHandler'");
  }

  @Override
  public void onNext(RemoteMessage<ReefServiceProtos.JobStatusProto> jobStatusProtoRemoteMessage) {
    this.runningJobs.onJobStatusMessage(jobStatusProtoRemoteMessage);
  }
}
