package com.microsoft.reef.runtime.common.driver.evaluator;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.runtime.common.driver.context.EvaluatorContext;
import com.microsoft.reef.runtime.common.protocol.ContextHeartbeat;
import com.microsoft.reef.runtime.common.protocol.ContextStateTransition;
import com.microsoft.reef.runtime.common.protocol.TaskHeartbeat;
import com.microsoft.reef.runtime.common.protocol.TaskStateTransition;
import com.microsoft.reef.util.Optional;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * Driver side representation of the ContextStack in an Evaluator.
 */
@DriverSide
@Private
public final class ContextManager {

  private final Map<String, EvaluatorContext> contextMap = new HashMap<>();
  private final Deque<EvaluatorContext> contextStack = new ArrayDeque<>();

  public synchronized EvaluatorContext pop() {
    final EvaluatorContext result = contextStack.pop();
    this.contextMap.remove(result.getId());
    return result;
  }

  public synchronized Optional<EvaluatorContext> peek() {
    return Optional.ofNullable(this.contextStack.peek());
  }

  public synchronized void push(final EvaluatorContext context) {
    this.contextStack.push(context);
    this.contextMap.put(context.getId(), context);
  }

  public synchronized Optional<EvaluatorContext> get(final String id) {
    return Optional.ofNullable(this.contextMap.get(id));
  }

  /**
   * @param id
   * @return true if the context with the given id is the top of the context stack.
   */
  public synchronized boolean isTopContext(final String id) {
    final boolean result;
    if (!this.contextMap.containsKey(id)) {
      result = false;
    } else {
      result = (this.contextStack.peek() == this.contextMap.get(id));
    }
    return result;
  }

  public synchronized void handleContextHeartbeat(final ContextHeartbeat heartbeat) {
    for (final ContextStateTransition transition : heartbeat.getStateTransitions()) {
      assert (transition.isLegal());
    }
    if (!contextMap.containsKey(heartbeat.getId())) { // this is a new context

    } else { // we know this context

    }
    // check whether we know this context
    // figure out whether we need to fire an event to the user.
  }

  public synchronized void handleTaskHeartbeat(final TaskHeartbeat heartbeat) {
    for (final TaskStateTransition transition : heartbeat.getStateTransitions()) {
      assert (transition.isLegal());
    }

  }
}
