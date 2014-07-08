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
package com.microsoft.reef.tests.context.failure;

import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.poison.PoisonedConfiguration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
public class FailureDriver {

  private static final int NUM_EVALUATORS = 1;

  private static final Logger LOG = Logger.getLogger(FailureDriver.class.getName());

  private final EvaluatorRequestor requestor;

  @Inject
  public FailureDriver(final EvaluatorRequestor requestor) {
    this.requestor = requestor;
    LOG.info("Driver instantiated");
  }

  /**
   * Handles the StartTime event: Request as single Evaluator.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.FINE, "Request {0} Evaluators.", NUM_EVALUATORS);
      FailureDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(NUM_EVALUATORS)
          .setMemory(64)
          .build());
    }
  }

  /**
   * Handles AllocatedEvaluator: Submit a poisoned context.
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final String evalId = allocatedEvaluator.getId();
      LOG.log(Level.FINE, "Got allocated evaluator: {0}", evalId);
      LOG.log(Level.FINE, "Submitting id context");
      allocatedEvaluator.submitContext(
          Tang.Factory.getTang()
              .newConfigurationBuilder(
                  ContextConfiguration.CONF
                      .set(ContextConfiguration.IDENTIFIER, "RootContext-" + evalId)
                      .build())
              .build());
    }
  }

  final class ContextActiveHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext activeContext) {
      final String ctxId = activeContext.getId();
      LOG.log(Level.FINE, "Got active context: {0}.", ctxId);
      if (ctxId.startsWith("Root")) {
        activeContext.submitContext(
            Tang.Factory.getTang().newConfigurationBuilder(
                ContextConfiguration.CONF
                    .set(ContextConfiguration.IDENTIFIER, "First-" + ctxId)
                    .build()
            ).build());
      } else if (ctxId.startsWith("First")) {
        activeContext.submitContext(
            Tang.Factory.getTang().newConfigurationBuilder(
                ContextConfiguration.CONF
                    .set(ContextConfiguration.IDENTIFIER, "Poisoned-" + ctxId)
                    .build(),
                PoisonedConfiguration.TASK_CONF
                    .set(PoisonedConfiguration.CRASH_PROBABILITY, "1")
                    .set(PoisonedConfiguration.CRASH_TIMEOUT, "4")
                    .build())
                .build());
      } else {
        activeContext.submitTask(
            TaskConfiguration.CONF
                .set(TaskConfiguration.IDENTIFIER, "EmptyTask-" + ctxId)
                .set(TaskConfiguration.TASK, FailureTask.class)
                .build());
      }
    }

  }

  /**
   * Handles FailedEvaluator: Resubmits the single Evaluator resource request.
   */
  final class TaskFailedHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask failedTask) {
      LOG.info("Got failed task - " + failedTask.getId() + " closing active context");
      failedTask.getActiveContext().get().close();
    }
  }
}
