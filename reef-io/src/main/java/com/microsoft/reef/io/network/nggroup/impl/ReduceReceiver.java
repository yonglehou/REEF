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

import java.util.List;

import javax.inject.Inject;

import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.nggroup.api.CommGroupNetworkHandler;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.DataCodec;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.Identifier;

/**
 * 
 */
public class ReduceReceiver<T> implements Reduce.Receiver<T> {
  
  private final String selfId;
  private final CommGroupNetworkHandler commGroupNetworkHandler;
  private final Codec<T> dataCodec;
  private final ReduceFunction<T> reduceFunction;
  
  
  @Inject
  public ReduceReceiver(
      @Parameter(TaskConfigurationOptions.Identifier.class) String selfId,
      @Parameter(DataCodec.class) Codec<T> dataCodec,
      @Parameter(com.microsoft.reef.io.network.nggroup.impl.config.parameters.ReduceFunction.class) ReduceFunction<T> reduceFunction,
      CommGroupNetworkHandler commGroupNetworkHandler) {
    super();
    this.selfId = selfId;
    this.dataCodec = dataCodec;
    this.reduceFunction = reduceFunction;
    this.commGroupNetworkHandler = commGroupNetworkHandler;
    //Create broacst handler and register with commGroupNetworkHandler
  }

  @Override
  public ReduceFunction<T> getReduceFunction() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public T reduce() throws InterruptedException, NetworkException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public T reduce(List<? extends Identifier> order)
      throws InterruptedException, NetworkException {
    // TODO Auto-generated method stub
    return null;
  }

}
