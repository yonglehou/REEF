package com.microsoft.reef.runtime.common.driver.evaluator;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.context.ContextMessage;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.EvaluatorType;
import com.microsoft.reef.runtime.common.driver.DispatchingEStage;
import com.microsoft.reef.runtime.common.driver.context.ContextMessageImpl;
import com.microsoft.reef.runtime.common.driver.context.EvaluatorContext;
import com.microsoft.reef.runtime.common.driver.context.FailedContextImpl;
import com.microsoft.reef.runtime.common.protocol.*;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.formats.ConfigurationSerializer;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

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

  private final EvaluatorManager evaluatorManager;
  private final ConfigurationSerializer configurationSerializer;
  private final Map<String, EvaluatorContext> contextMap = new HashMap<>();
  private final Deque<EvaluatorContext> contextStack = new ArrayDeque<>();
  private final DispatchingEStage dispatcher;

  public ContextManager(final EvaluatorManager evaluatorManager,
                        final ConfigurationSerializer configurationSerializer,
                        final DispatchingEStage dispatcher) {
    this.evaluatorManager = evaluatorManager;
    this.configurationSerializer = configurationSerializer;
    this.dispatcher = dispatcher;
  }


  private synchronized EvaluatorContext pop() {
    final EvaluatorContext result = contextStack.pop();
    this.contextMap.remove(result.getId());
    return result;
  }

  public synchronized Optional<EvaluatorContext> peek() {
    return Optional.ofNullable(this.contextStack.peek());
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

  public synchronized boolean isEmpty() {
    final boolean result = this.contextStack.isEmpty();
    if (this.contextMap.isEmpty() != result) {
      throw new IllegalStateException("Inconsistent state.");
    }
    return result;
  }

  public synchronized void handleContextHeartbeat(final ContextHeartbeat heartbeat) {
    Optional<ContextHeartbeat> hb = Optional.of(heartbeat);
    while (hb.isPresent()) {
      processContextHeartbeat(hb.get());
      hb = hb.get().getParentHeartbeat();
    }
  }

  private synchronized void processContextHeartbeat(final ContextHeartbeat heartbeat) {
    assert (isSane(heartbeat));
    final String contextId = heartbeat.getId();

    if (!this.contextMap.containsKey(contextId)) {
      // Register a new context.
      this.onNewContext(heartbeat);
    } else {
      final EvaluatorContext ctx = get(heartbeat.getId()).get();
      if (!ctx.getState().equals(heartbeat.getState())) {
        switch (heartbeat.getState()) {
          case READY:
            throw new IllegalStateException("Transition from `" + ctx.getState() + "` to `" + heartbeat.getState() + "` is not supported");
          case DONE:
            this.onContextClose(heartbeat);
            break;
          case FAILED:
            this.onContextFail(heartbeat);
            break;
          default:
            throw new IllegalArgumentException("Unknown state: " + heartbeat.getState());
        }
      }
    }

    // Send messages upstream
    processMessages(heartbeat);
  }


  private void onContextClose(final ContextHeartbeat heartbeat) {
    assert (heartbeat.getState().equals(ContextState.DONE));
    final String contextId = heartbeat.getId();
    if (!this.peek().get().getParentId().equals(heartbeat.getId())) {
      throw new IllegalStateException("Received ClosedContext for `" + contextId + "` which is not the top context.");
    }
    final EvaluatorContext ctx = this.pop();
    if (this.peek().isPresent()) {
      dispatcher.onNext(ClosedContext.class, ctx.getClosedContext(this.peek().get()));
    } else {
      throw new IllegalStateException("Received a closedcontext for the root context. We should instead have gotten a CompletedEvaluator.");
    }
  }

  private void onContextFail(final ContextHeartbeat heartbeat) {
    assert (heartbeat.getState().equals(ContextState.FAILED));
    final String contextId = heartbeat.getId();
    if (!this.peek().get().getParentId().equals(contextId)) {
      throw new IllegalStateException("Received FailedContext for `" + contextId + "` which is not the top context.");
    }
    final EvaluatorContext ctx = this.pop();


    final Optional<Throwable> cause;
    final Optional<byte[]> data;
    final String message;
    final Optional<String> description;
    if (heartbeat.getError().isPresent()) {
      final ErrorMessage errorMessage = heartbeat.getError().get();
      message = errorMessage.getShortMessage();
      description = errorMessage.getLongMessage();
      if (errorMessage.getSerializedException().isPresent()) {
        data = errorMessage.getSerializedException();
        if (errorMessage.getType().equals(EvaluatorType.JVM)) {
          cause = Optional.of(new ObjectSerializableCodec<Throwable>().decode(data.get()));
        } else {
          cause = Optional.empty();
        }
      } else {
        data = Optional.empty();
        cause = Optional.empty();
      }
    } else {
      message = "UNKNOWN_ERROR";
      description = Optional.empty();
      cause = Optional.empty();
      data = Optional.empty();
    }

    final Optional<ActiveContext> parentContext;
    if (this.peek().isPresent()) {
      final ActiveContext parent = this.peek().get();
      parentContext = Optional.of(parent);
    } else {
      parentContext = Optional.empty();
    }

    final FailedContext failedContext = new FailedContextImpl(contextId, message, description, cause, data,
        parentContext, ctx.getEvaluatorDescriptor(), ctx.getEvaluatorId());
    dispatcher.onNext(FailedContext.class, failedContext);
  }


  /**
   * Process the messages in the heartbeat
   *
   * @param heartbeat
   */
  private void processMessages(final ContextHeartbeat heartbeat) {
    final String contextId = heartbeat.getId();
    for (final Message message : heartbeat.getMessages()) {
      dispatcher.onNext(ContextMessage.class, new ContextMessageImpl(message.getMessage(), contextId, message.getSourceIdentifier()));
    }
  }

  private void onNewContext(final ContextHeartbeat heartbeat) {
    final EvaluatorContext result;
    if (this.isEmpty()) {
      result = new EvaluatorContext(evaluatorManager, heartbeat.getId(), Optional.<String>empty(), configurationSerializer);
    } else {
      if (!heartbeat.getParentHeartbeat().isPresent()) {
        throw new IllegalStateException("Received a new context whose parent isn't set.");
      }
      final String parentId = heartbeat.getParentHeartbeat().get().getId();
      if (!this.isTopContext(parentId)) {
        final String realTopId = peek().get().getId();
        throw new IllegalStateException("Received a new context whose parent is `" + parentId + "`, but the real top id is `" + realTopId + "`");
      }
      result = new EvaluatorContext(evaluatorManager, heartbeat.getId(), Optional.of(parentId), configurationSerializer);
    }
    this.contextStack.push(result);
    this.contextMap.put(result.getId(), result);
    this.dispatcher.onNext(ActiveContext.class, result);
  }

  private boolean isSane(final ContextHeartbeat heartbeat) {
    for (final ContextStateTransition transition : heartbeat.getStateTransitions()) {
      if (!transition.isLegal()) {
        return false;
      }
    }
    // TODO: Think about more checks
    return true;
  }

  public synchronized void handleTaskHeartbeat(final TaskHeartbeat heartbeat) {
    for (final TaskStateTransition transition : heartbeat.getStateTransitions()) {
      assert (transition.isLegal());
    }
    // TODO

  }
}
