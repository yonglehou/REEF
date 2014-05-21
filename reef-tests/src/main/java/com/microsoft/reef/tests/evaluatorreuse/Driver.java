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
package com.microsoft.reef.tests.evaluatorreuse;

import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.client.JobMessageObserver;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.tests.exceptions.UnexpectedTaskReturnValue;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import javax.xml.bind.DatatypeConverter;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
final class Driver {

  private static final Logger LOG = Logger.getLogger(Driver.class.getName());

  private final int numberOfIterations;
  private final JobMessageObserver client;

  private int counter = 0;
  private String lastMessage = null;

  @NamedParameter(default_value = "10", short_name = "i")
  class NumberOfIterations implements Name<Integer> {
  }

  @Inject
  Driver(final @Parameter(NumberOfIterations.class) int n,
         final JobMessageObserver client) {
    this.numberOfIterations = n;
    this.client = client;
  }

  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask completed) {
      final String returned = new String(completed.get());
      final String msg = "CompletedTask returned: \"" + returned + "\"";
      client.onNext(msg.getBytes());
      if (!returned.equals(lastMessage)) {
        throw new UnexpectedTaskReturnValue(lastMessage, returned);
      } else {
        startTask(completed.getActiveContext());
      }
    }
  }

  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eb) {
      LOG.log(Level.FINE, "AllocatedEvaluator: " + eb);
      try {
        eb.submitContext(ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, "EvaluatorReuse").build());
      } catch (BindException e) {
        throw new RuntimeException(e);
      }
    }
  }

  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      startTask(context);
    }
  }

  private void startTask(final ActiveContext context) {
    if (counter < numberOfIterations) {
      try {
        this.lastMessage = "ECHO-" + counter;
        client.onNext(("Submitting iteration " + counter).getBytes());
        final String memento = DatatypeConverter.printBase64Binary(this.lastMessage.getBytes());
        context.submitTask(TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, this.lastMessage)
            .set(TaskConfiguration.TASK, EchoTask.class)
            .set(TaskConfiguration.MEMENTO, memento)
            .build());
        counter += 1;
      } catch (final BindException e) {
        context.close();
        throw new RuntimeException(e);
      }
    } else {
      client.onNext("Done. Closing the Context".getBytes());
      context.close();
    }
  }
}
