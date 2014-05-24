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
package com.microsoft.reef.io.network.nggroup.api;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;

/**
 *
 */
public interface OperatorHandler {
  /**
   * @return
   */
  NeighborStatus updateTopology();

  /**
   *
   */
  void waitForSetup();

  /**
   * @return
   */
  CountDownLatch getParentLatch();

  /**
   * @return
   */
  CountDownLatch getChildLatch();

  /**
   * @return
   */
  BlockingQueue<GroupCommMessage> getCtrlQue();

  /**
   * @return
   */
  ConcurrentMap<String, BlockingQueue<GroupCommMessage>> getDataQue();

  /**
   * @param srcId
   */
  void addNeighbor(String srcId);

  /**
   * @param srcId
   */
  void removeNeighbor(String srcId);
}
