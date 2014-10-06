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
package com.microsoft.reef.tests.close_eval;

import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
final class CloseEvaluatorDriver {

  private static final Logger LOG = Logger.getLogger(CloseEvaluatorDriver.class.getName());

  private static final int NUM_EVALUATORS = 16;

  private final EvaluatorRequestor requestor;

  @Inject
  CloseEvaluatorDriver(final EvaluatorRequestor requestor) {
    this.requestor = requestor;
  }

  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime time) {

      LOG.log(Level.FINE, "StartTime: {0} :: request {1} evaluators",
          new Object[]{time, NUM_EVALUATORS});

      requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(NUM_EVALUATORS).setMemory(256).setNumberOfCores(1).build());
    }
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eval) {
      LOG.log(Level.FINE, "Allocated Evaluator: {0} :: closing", eval);
      eval.close();
    }
  }
}
