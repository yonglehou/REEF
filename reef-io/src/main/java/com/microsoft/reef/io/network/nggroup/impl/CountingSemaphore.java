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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 *
 */
public class CountingSemaphore {

  private static final Logger LOG = Logger.getLogger(CountingSemaphore.class.getName());


  private final AtomicInteger counter;

  public CountingSemaphore(final int initCount) {
    super();
    this.counter = new AtomicInteger(initCount);
    LOG.info("Counter initialized to " + initCount);
  }

  public synchronized int increment() {
    LOG.info("Incrementing counter");
    return counter.incrementAndGet();
  }

  public synchronized int decrement() {
    LOG.info("Decrementing counter");
    final int retVal = counter.decrementAndGet();
    if(retVal<0) {
      LOG.warning("Counter negative. Something fishy");
    }
    if(retVal==0) {
      LOG.info("All workers are done with their task. Notifying waiting threads");
      notifyAll();
    } else {
      LOG.info("Some workers are not done yet");
    }
    return retVal;
  }

  public synchronized void await() {
    LOG.info("Waiting for workers to be done");
    while(counter.get()!=0) {
      try {
        wait();
      } catch (final InterruptedException e) {
        throw new RuntimeException("InterruptedException while waiting for counting semaphore counter", e);
      }
    }
    LOG.info("Returning from wait");
  }

}
