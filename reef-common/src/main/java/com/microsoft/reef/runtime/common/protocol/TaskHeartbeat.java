package com.microsoft.reef.runtime.common.protocol;

import com.microsoft.reef.util.Optional;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A Task heartbeat sent from the Evaluator to the Driver.
 */
public final class TaskHeartbeat {
  /**
   * The task ID.
   */
  private final String id;

  /**
   * The current state of the task.
   */
  private final TaskState state;
  /**
   * All state transitions of this task.
   */
  private final Collection<TaskStateTransition> stateTransitions;

  /**
   * The error, if there was one.
   */
  private final Optional<ErrorProtocol> error;

  /**
   * Messages from the Task.
   */
  private final Iterable<Message> messages;

  public TaskHeartbeat(final String id,
                       final TaskState state,
                       final Collection<TaskStateTransition> stateTransitions,
                       final Optional<ErrorProtocol> error,
                       final Iterable<Message> messages) {
    this.id = id;
    this.state = state;
    this.stateTransitions = stateTransitions;
    this.error = error;
    this.messages = messages;
  }

  public TaskHeartbeat(final String id,
                       final TaskState state,
                       final Collection<TaskStateTransition> stateTransitions) {
    this(id, state, stateTransitions, Optional.<ErrorProtocol>empty(), new ArrayList<Message>(0));
  }

  public TaskHeartbeat(final String id,
                       final TaskState state,
                       final Collection<TaskStateTransition> stateTransitions,
                       final Optional<ErrorProtocol> error) {
    this(id, state, stateTransitions, error, new ArrayList<Message>(0));
  }

  public TaskHeartbeat(final String id,
                       final TaskState state,
                       final Collection<TaskStateTransition> stateTransitions,
                       final ErrorProtocol error) {
    this(id, state, stateTransitions, Optional.of(error), new ArrayList<Message>(0));
  }

  public TaskHeartbeat(final String id,
                       final TaskState state,
                       final Collection<TaskStateTransition> stateTransitions,
                       final Iterable<Message> messages) {
    this(id, state, stateTransitions, Optional.<ErrorProtocol>empty(), messages);

  }

  /**
   * @return The Task id.
   */
  public String getId() {
    return id;
  }

  /**
   * @return the current state of the task.
   */
  public TaskState getState() {
    return state;
  }

  /**
   * @return the state transitions of this task.
   */
  public Collection<TaskStateTransition> getStateTransitions() {
    return stateTransitions;
  }

  /**
   * @return the error, if there was one.
   */
  public Optional<ErrorProtocol> getError() {
    return error;
  }

  /**
   * @return the messages sent by the Task, if any.
   */
  public Iterable<Message> getMessages() {
    return messages;
  }
}
