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
package com.microsoft.reef.tests.evaluatorfailure;

import com.microsoft.reef.evaluator.context.events.ContextStart;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.Clock;
import com.microsoft.wake.time.event.Alarm;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A handler for ContextStart that schedules an Alarm handler that throws an Exception.
 */
final class FailureSchedulingContextStartHandler implements EventHandler<ContextStart> {
  private static final Logger LOG = Logger.getLogger(FailureSchedulingContextStartHandler.class.getName());
  private final Clock clock;

  @Inject
  FailureSchedulingContextStartHandler(final Clock clock) {
    this.clock = clock;
  }

  @Override
  public void onNext(final ContextStart contextStart) {
    this.clock.scheduleAlarm(0, new EventHandler<Alarm>() {
      @Override
      public void onNext(Alarm alarm) {
        LOG.log(Level.INFO, "Invoked for {0}, throwing an Exception now.", alarm);
        throw new ExpectedException();
      }
    });
  }
}
