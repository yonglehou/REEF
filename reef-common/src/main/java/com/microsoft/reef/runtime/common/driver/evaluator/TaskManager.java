package com.microsoft.reef.runtime.common.driver.evaluator;

import com.microsoft.reef.runtime.common.protocol.TaskHeartbeat;

import javax.inject.Inject;

/**
 * Created by mweimer on 2014-04-02.
 */
final class TaskManager {

  private final EvaluatorMessageDispatcher messageDispatcher;

  @Inject
  TaskManager(EvaluatorMessageDispatcher messageDispatcher) {
    this.messageDispatcher = messageDispatcher;
  }


  void handleTaskHeartbeat(final TaskHeartbeat heartbeat) {

  }
}
