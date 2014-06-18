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

import java.util.logging.Logger;

import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.reef.io.network.nggroup.api.Topology;
import com.microsoft.wake.EventHandler;

/**
 *
 */
public class TopologyRunningTaskHandler implements EventHandler<RunningTask> {

  private static final Logger LOG = Logger.getLogger(TopologyRunningTaskHandler.class.getName());

  private final Topology topology;

  public TopologyRunningTaskHandler(final Topology topology) {
    this.topology = topology;
  }

  @Override
  public void onNext(final RunningTask runningTask) {
    LOG.info("Got running task: " + runningTask.getId());
    topology.setRunning(runningTask.getId());
  }

}
