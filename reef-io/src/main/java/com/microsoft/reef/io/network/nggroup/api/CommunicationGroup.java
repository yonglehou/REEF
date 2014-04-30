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

import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.app.ControlMessages;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.reef.task.Task;
import com.microsoft.tang.Configuration;

/**
 * 
 */
public interface CommunicationGroup {

  /**
   * @param string
   * @param dataCodec 
   * @return
   */
  CommunicationGroup addBroadcast(String string, Codec<?> dataCodec);

  /**
   * @param string
   * @param dataCodec 
   * @return
   */
  CommunicationGroup addReduce(String string, Codec<?> dataCodec);

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
   * marks whether this active context
   * should be a slave
   * @param activeContext
   */
  void addSlaveContext(ActiveContext activeContext);

  /**
   * marks whether this active context
   * should be a master
   * @param activeContext
   */
  void addMasterContext(ActiveContext activeContext);

  /**
   * @param activeContext
   * @return
   */
  Configuration getContextConf(ActiveContext activeContext);

  /**
   * @param activeContext
   * @return
   */
  Configuration getServiceConf(ActiveContext activeContext);

  
  /***********************************************************/
  //Client Side APIs
  /***********************************************************/
  /**
   * @param string
   * @return
   */
  Broadcast.Sender getBroadcastSender(String string);

  /**
   * @param string
   * @return
   */
  Reduce.Receiver getReduceReceiver(String string);
  
  /**
   * @param string
   * @return
   */
  Broadcast.Receiver getBroadcastReceiver(String string);
  
  /**
   * @param string
   * @return
   */
  Reduce.Sender getReduceSender(String string);

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
   * blocking call that waits for all the participants
   * of this group to be available
   */
  void waitForAll();
}
