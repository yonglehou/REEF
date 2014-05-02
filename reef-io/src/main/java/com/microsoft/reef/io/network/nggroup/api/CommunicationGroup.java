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

import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.nggroup.app.ControlMessages;
import com.microsoft.reef.io.network.nggroup.app.Vector;
import com.microsoft.reef.io.network.nggroup.app.parameters.ControlMessageBroadcaster;
import com.microsoft.reef.io.network.nggroup.app.parameters.ModelAndDescentDirectionBroadcaster;
import com.microsoft.reef.io.network.util.Utils.Pair;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.reef.task.Task;
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
  CommunicationGroup addBroadcast(Class<? extends Name<String>> operatorName, Codec<?> dataCodec);

  /**
   * @param string
   * @param dataCodec 
   * @param reduceFunction 
   * @return
   */
  CommunicationGroup addReduce(Class<? extends Name<String>> operatorName, Codec<?> dataCodec, ReduceFunction<?> reduceFunction);

  /**
   * 
   */
  void finalize();

  /**
   * @param activeContext
   * @return
   */
  boolean isMaster(ActiveContext activeContext);

  /**
   * @param activeContext
   * @param taskClazz
   */
  void submitTask(ActiveContext activeContext, Class<? extends Task> taskClazz);

  
  /**
   * @param string
   */
  void setSenderId(String senderId);


  
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
  void waitFor(int numberOfReceivers, int timeout, TimeUnit unit);
  
  /**
   * blocking call that waits for timeout timeunits
   * @param numberOfReceivers
   * @param timeout
   * @param unit
   */
  void waitFor(int timeout, TimeUnit unit);

  /**
   * @param operatorName
   * @return
   */
  Configuration getSenderConfiguration(Class<? extends Name<String>> operatorName);

  /**
   * @param operatorName
   * @param slaveId 
   * @return
   */
  Configuration getReceiverConfiguration(Class<? extends Name<String>> operatorName, String slaveId);
}
