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
package com.microsoft.reef.io.network.nggroup.api;

import java.util.concurrent.ExecutionException;

import com.microsoft.reef.io.network.group.operators.AllReduce;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.tang.annotations.DefaultImplementation;
import com.microsoft.tang.annotations.Name;

/**
 *
 */
@DefaultImplementation(value = com.microsoft.reef.io.network.nggroup.impl.CommunicationGroupClientImpl.class)
public interface CommunicationGroupClient {

  /***********************************************************/
  //Client Side APIs
  /***********************************************************/
  /**
   * @param operatorName
   * @return
   */
  Broadcast.Sender getBroadcastSender(Class<? extends Name<String>> operatorName);

  /**
   * @param operatorName
   * @return
   */
  Reduce.Receiver getReduceReceiver(Class<? extends Name<String>> operatorName);

  /**
   * @param operatorName
   * @return
   */
  Broadcast.Receiver getBroadcastReceiver(Class<? extends Name<String>> operatorName);

  /**
   * @param operatorName
   * @return
   */
  Reduce.Sender getReduceSender(Class<? extends Name<String>> operatorName);
  
  /**
   * @param operatorName
   * @return
   */
  AllReduce getAllReducer(Class<? extends Name<String>> operatorName);

  /**
   *
   */
  void initialize();

  /**
   * @return
   */
  GroupChanges getTopologyChanges();

  /**
   *
   */
  void updateTopology();

  /**
   * @return
   */
  Class<? extends Name<String>> getName();

  // The following interfaces are added for allreduce operators.
  
  String getTaskID();

  void checkIteration();

  boolean isCurrentIterationFailed() throws UnsupportedOperationException;

  boolean isNewTaskComing() throws UnsupportedOperationException;

  void updateIteration();
}
