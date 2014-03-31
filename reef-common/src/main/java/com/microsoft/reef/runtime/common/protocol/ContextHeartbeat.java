package com.microsoft.reef.runtime.common.protocol;

import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.util.Optional;

import java.util.Collection;

/**
 * Context heartbeat sent from the Evaluator to the Driver.
 */
@Private
public final class ContextHeartbeat {

  /**
   * State transitions of this context.
   */
  private final Collection<ContextStateTransition> stateTransitions;

  /**
   * The current state of this context.
   */
  private final ContextState state;

  /**
   * The ID of this context.
   */
  private final String id;

  /**
   * The parent heartbeat, if any.
   */
  private final Optional<ContextHeartbeat> parentHeartbeat;

  /**
   * The error, if there was one.
   */
  private final Optional<ErrorMessage> error;

  /**
   * Messages from the Context.
   */
  private final Iterable<Message> messages;

  /**
   * @param stateTransitions state transitions of the Context
   * @param state            the current state of the Context
   * @param id               the ID of the Context.
   * @param parentHeartbeat  the heartbeat of the parent Context, if any.
   * @param error            the error thrown by the Context, if any.
   * @param messages         the messages sent by the Context, if any.
   */
  public ContextHeartbeat(final Collection<ContextStateTransition> stateTransitions,
                          final ContextState state,
                          final String id,
                          final Optional<ContextHeartbeat> parentHeartbeat,
                          final Optional<ErrorMessage> error,
                          final Iterable<Message> messages) {
    this.stateTransitions = stateTransitions;
    this.state = state;
    this.id = id;
    this.parentHeartbeat = parentHeartbeat;
    this.error = error;
    this.messages = messages;
  }

  /**
   * @return State transitions of this Context.
   */
  public Collection<ContextStateTransition> getStateTransitions() {
    return stateTransitions;
  }

  /**
   * @return the current state of the Context.
   */
  public ContextState getState() {
    return state;
  }

  /**
   * @return the id of the context.
   */
  public String getId() {
    return id;
  }

  /**
   * @return the heartbeat of the parent Context, if any.
   */
  public Optional<ContextHeartbeat> getParentHeartbeat() {
    return parentHeartbeat;
  }

  /**
   * @return the error thrown by this Context, if any.
   */
  public Optional<ErrorMessage> getError() {
    return error;
  }

  /**
   * @return the messages sent by this Context, if any.
   */
  public Iterable<Message> getMessages() {
    return messages;
  }
}
