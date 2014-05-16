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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Name;

/**
 * 
 */
public interface CommunicationGroup {

  /**
   * @param string
   * @param dataCodec 
   * @return
   */
  CommunicationGroup addBroadcast(Class<? extends Name<String>> operatorName, BroadcastOperatorSpec spec);

  /**
   * @param string
   * @param dataCodec 
   * @param reduceFunction 
   * @return
   */
  CommunicationGroup addReduce(Class<? extends Name<String>> operatorName, ReduceOperatorSpec spec);

  /**
   * 
   */
  void finalize();

  /**
   * @param build
   * @return
   */
  Configuration getConfiguration(Configuration taskConf);


  
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
   * Non-blocking call to find if there are
   * any changes to the composition of this
   * group. This should also invoke a synchronize
   * on all the operators running in the participants
   * of this group. Once this call returns
   * the operators are configured to deal with
   * the current network topology and the
   * control queue of the operators are empty
   * @return
   */
  GroupChanges synchronize();
  
  /**
   * blocking call that waits for the numberOfReceivers participants
   * of this group to be available and times out after timeout timeunits
   * @param numberOfReceivers
   * @param timeout
   * @param unit
   */
  void waitFor(int numberOfReceivers, int timeout, TimeUnit unit) throws TimeoutException;
  
  /**
   * blocking call that waits for timeout timeunits
   * @param numberOfReceivers
   * @param timeout
   * @param unit
   */
  void waitFor(int timeout, TimeUnit unit) throws TimeoutException;
}
