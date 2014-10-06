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
package com.microsoft.reef.runtime.common.driver.evaluator;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.util.Optional;
import com.microsoft.reef.util.SingletonAsserter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages all Evaluators.
 * See EvaluatorManager for the Driver side representation of a single Evaluator.
 */
@DriverSide
@Private
public final class Evaluators implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(Evaluators.class.getName());

  /**
   * A map between evaluatorId and the EvaluatorManager that handles this evaluator.
   */
  private final Map<String, EvaluatorManager> evaluators = new HashMap<>();


  @Inject
  Evaluators() {
    LOG.log(Level.FINE, "Instantiated 'Evaluators'");
    assert (SingletonAsserter.assertSingleton(Evaluators.class));
  }

  /**
   * Closes all EvaluatorManager instances managed.
   */
  @Override
  public void close() {
    final List<EvaluatorManager> evaluatorsCopy;
    synchronized (this) {
      evaluatorsCopy = new ArrayList<>(this.evaluators.values());
    }
    for (final EvaluatorManager evaluatorManager : evaluatorsCopy) {
      LOG.log(Level.WARNING, "Unclean shutdown of evaluator {0}", evaluatorManager.getId());
      if (!evaluatorManager.isClosed()) {
        evaluatorManager.close();
      }
    }
  }

  /**
   * Return true if <em>all</em> evaluators are in closed state
   * (and their processing queues are empty).
   */
  public synchronized boolean allEvaluatorsAreClosed() {
    synchronized (this.evaluators) {
      for (final EvaluatorManager eval : this.evaluators.values()) {
        if (!eval.isClosed()) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * @param evaluatorId
   * @return the EvaluatorManager for the given id, if one exists.
   */
  public synchronized Optional<EvaluatorManager> get(final String evaluatorId) {
    return Optional.ofNullable(this.evaluators.get(evaluatorId));
  }

  /**
   * Create new EvaluatorManager and add it to the collection.
   * <p/>
   * FIXME: This method is a temporary fix for the race condition
   * described in issues #828 and #839.
   *
   * @param evaluatorManagerFactory Factory that builds new EvaluatorManager objects.
   * @param evaluatorMsg            Resource allocation message that contains data on the new evaluator.
   * @throws java.lang.IllegalArgumentException if the EvaluatorManager is already known.
   */
  public synchronized void put(
      final EvaluatorManagerFactory evaluatorManagerFactory,
      final DriverRuntimeProtocol.ResourceAllocationProto evaluatorMsg) {
    this.put(evaluatorManagerFactory.getNewEvaluatorManager(evaluatorMsg));
  }

  /**
   * Adds an EvaluatorManager.
   *
   * @param evaluatorManager
   * @throws java.lang.IllegalArgumentException if the EvaluatorManager is already known.
   */
  public synchronized void put(final EvaluatorManager evaluatorManager) {
    final String evaluatorId = evaluatorManager.getId();
    final EvaluatorManager prev = this.evaluators.put(evaluatorId, evaluatorManager);
    LOG.log(Level.FINEST, "Adding: {0} previous: {1}", new Object[]{evaluatorId, prev});
    if (prev != null) {
      throw new IllegalArgumentException(
          "Trying to re-add an Evaluator that is already known: " + evaluatorId);
    }
  }
}
