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

import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.io.network.nggroup.api.Topology;
import com.microsoft.wake.EventHandler;

/**
 *
 */
public class TopologyFailedTaskHandler implements EventHandler<FailedTask> {

  private final Topology topology;

  public TopologyFailedTaskHandler(final Topology topology) {
    this.topology = topology;
  }

  @Override
  public void onNext(final FailedTask failedTask) {
    topology.setFailed(failedTask.getId());
  }

}
