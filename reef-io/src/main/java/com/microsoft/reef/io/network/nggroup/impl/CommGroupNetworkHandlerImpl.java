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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Inject;

import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.tang.annotations.Name;
import com.microsoft.wake.EventHandler;

/**
 *
 */
public class CommGroupNetworkHandlerImpl implements com.microsoft.reef.io.network.nggroup.api.CommGroupNetworkHandler{
  private final Map<Class<? extends Name<String>>, EventHandler<GroupCommMessage>> operHandlers = new ConcurrentHashMap<>();

  @Inject
  public CommGroupNetworkHandlerImpl() {  }

  @Override
  public void onNext(final GroupCommMessage msg) {
    final Class<? extends Name<String>> operName = Utils.getClass(msg
        .getOperatorname());
    operHandlers.get(operName).onNext(msg);
  }

  @Override
  public void register(final Class<? extends Name<String>> operName,
      final EventHandler<GroupCommMessage> operHandler) {
    operHandlers.put(operName, operHandler);
  }

}
