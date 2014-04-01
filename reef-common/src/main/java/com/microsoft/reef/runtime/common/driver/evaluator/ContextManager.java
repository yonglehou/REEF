package com.microsoft.reef.runtime.common.driver.evaluator;

import com.microsoft.reef.runtime.common.driver.context.EvaluatorContext;
import com.microsoft.reef.runtime.common.protocol.ContextHeartbeat;
import com.microsoft.reef.util.Optional;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

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

  public synchronized void handleContextHeartbeat(final ContextHeartbeat heartbeat) {
    // check whether we know this context
    // figure out whether we need to fire an event to the user.
  }
}
