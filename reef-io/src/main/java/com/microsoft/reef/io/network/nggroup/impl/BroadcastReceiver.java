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

import javax.inject.Inject;

import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.group.operators.Broadcast.Receiver;
import com.microsoft.reef.io.network.nggroup.api.CommGroupNetworkHandler;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.DataCodec;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.tang.annotations.Parameter;

/**
 * 
 */
public class BroadcastReceiver<T> implements Receiver<T> {
  
  private final String selfId;
  private final CommGroupNetworkHandler commGroupNetworkHandler;
  private final Codec<T> dataCodec;
  
  
  @Inject
  public BroadcastReceiver(
      @Parameter(TaskConfigurationOptions.Identifier.class) String selfId,
      @Parameter(DataCodec.class) Codec<T> dataCodec,
      CommGroupNetworkHandler commGroupNetworkHandler) {
    super();
    this.selfId = selfId;
    this.dataCodec = dataCodec;
    this.commGroupNetworkHandler = commGroupNetworkHandler;
    //Create broacst handler and register with commGroupNetworkHandler
  }


  @Override
  public T receive() throws NetworkException, InterruptedException {
    // TODO Auto-generated method stub
    return null;
  }

}
