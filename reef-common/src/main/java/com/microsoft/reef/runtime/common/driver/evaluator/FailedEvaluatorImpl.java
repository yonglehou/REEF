/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.runtime.common.driver.evaluator;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.exception.EvaluatorException;
import com.microsoft.reef.util.Optional;

import java.util.List;

@DriverSide
@Private
final class FailedEvaluatorImpl implements FailedEvaluator {

  private final String id;
  private final String message;
  private final Optional<String> description;
  private final Optional<Throwable> cause;
  private final Optional<byte[]> data;
  private final List<FailedContext> ctx;
  private final Optional<FailedTask> task;


  FailedEvaluatorImpl(final String id,
                      final String message,
                      final Optional<String> description,
                      final Optional<Throwable> cause,
                      final Optional<byte[]> data,
                      final List<FailedContext> ctx,
                      final Optional<FailedTask> task) {
    this.id = id;
    this.message = message;
    this.description = description;
    this.cause = cause;
    this.data = data;
    this.ctx = ctx;
    this.task = task;
  }

  /**
   * @param cause
   * @param ctx
   * @param task
   * @param id
   * @deprecated use the main constructor instead
   */
  @Deprecated
  FailedEvaluatorImpl(final Throwable cause, final List<FailedContext> ctx, final Optional<FailedTask> task, final String id) {
    this(id, cause.getMessage(), Optional.<String>empty(), Optional.of(cause), Optional.<byte[]>empty(), ctx, task);
  }

  @Override
  public EvaluatorException getEvaluatorException() {
    if (this.cause.isPresent()) {
      return new EvaluatorException(this.getId(), this.getMessage(), this.cause.get());
    } else {
      return new EvaluatorException(this.getId(), this.getMessage());
    }
  }


  @Override
  public List<FailedContext> getFailedContextList() {
    return this.ctx;
  }

  @Override
  public Optional<FailedTask> getFailedTask() {
    return this.task;
  }


  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public String toString() {
    return "FailedEvaluator{" +
        "id='" + id + '\'' +
        '}';
  }

  @Override
  public String getMessage() {
    return this.message;
  }

  @Override
  public Optional<String> getDescription() {
    return this.description;
  }

  @Override
  public Throwable getCause() {
    return this.cause.orElse(null);
  }

  @Override
  public Optional<Throwable> getReason() {
    return this.cause;
  }

  @Override
  public Optional<byte[]> getData() {
    return this.data;
  }

  @Override
  public Throwable asError() {
    return this.cause.isPresent() ? this.cause.get() : new RuntimeException(this.toString());
  }
}
