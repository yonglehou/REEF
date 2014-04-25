/**
 * Copyright (C) 2013 Microsoft Corporation
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

import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.RemoteMessage;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Used in REEFImplementation.
 */
final class RuntimeErrorProtoHandler implements EventHandler<RemoteMessage<ReefServiceProtos.RuntimeErrorProto>> {

  private final static Logger LOG = Logger.getLogger(RuntimeErrorProtoHandler.class.getName());

  private final InjectionFuture<RunningJobs> runningJobs;

  @Inject
  RuntimeErrorProtoHandler(final InjectionFuture<RunningJobs> runningJobs) {
    this.runningJobs = runningJobs;
  }


  @Override
  public void onNext(final RemoteMessage<ReefServiceProtos.RuntimeErrorProto> error) {
    LOG.log(Level.WARNING, "{0} Runtime Error: {1}", new Object[]{
        error.getIdentifier(), error.getMessage().getMessage()});
    this.runningJobs.get().onRuntimeErrorMessage(error);
  }
}
