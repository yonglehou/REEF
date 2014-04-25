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
package com.microsoft.reef.runtime.local.client;

import com.microsoft.tang.ExternalConstructor;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Used to make instances of ExecutorService via tang.
 */
public final class ExecutorServiceConstructor implements ExternalConstructor<ExecutorService> {
  private static final Logger LOG = Logger.getLogger(ExecutorServiceConstructor.class.getName());

  @Inject
  ExecutorServiceConstructor() {
  }

  @Override
  public ExecutorService newInstance() {
    LOG.log(Level.INFO, "Instantiating new 'ExecutorService'.");
    return Executors.newCachedThreadPool();
  }
}
