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
package com.microsoft.reef.io.network.nggroup.impl;

import com.microsoft.wake.EventHandler;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class BroadcastingEventHandler<T> implements EventHandler<T> {

  List<EventHandler<T>> handlers = new ArrayList<>();

  public void addHandler(final EventHandler<T> handler) {
    handlers.add(handler);
  }

  @Override
  public void onNext(final T msg) {
    for (final EventHandler<T> handler : handlers) {
      handler.onNext(msg);
    }
  }

}
