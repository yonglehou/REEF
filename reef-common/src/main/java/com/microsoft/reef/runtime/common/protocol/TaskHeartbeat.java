package com.microsoft.reef.runtime.common.protocol;

import com.microsoft.reef.util.Optional;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
   * The errorMessage, if there was one.
   */
  private final Optional<ErrorMessage> errorMessage;

  /**
   * Messages from the Task.
   */
  private final List<Message> messages;

  public TaskHeartbeat(final String id,
                       final TaskState state,
                       final Collection<TaskStateTransition> stateTransitions,
                       final Optional<ErrorMessage> errorMessage,
                       final List<Message> messages) {
    this.id = id;
    this.state = state;
    this.stateTransitions = stateTransitions;
    this.errorMessage = errorMessage;
    this.messages = messages;
  }

  public TaskHeartbeat(final String id,
                       final TaskState state,
                       final Collection<TaskStateTransition> stateTransitions) {
    this(id, state, stateTransitions, Optional.<ErrorMessage>empty(), new ArrayList<Message>(0));
  }

  public TaskHeartbeat(final String id,
                       final TaskState state,
                       final Collection<TaskStateTransition> stateTransitions,
                       final Optional<ErrorMessage> errorMessage) {
    this(id, state, stateTransitions, errorMessage, new ArrayList<Message>(0));
  }

  public TaskHeartbeat(final String id,
                       final TaskState state,
                       final Collection<TaskStateTransition> stateTransitions,
                       final ErrorMessage errorMessage) {
    this(id, state, stateTransitions, Optional.of(errorMessage), new ArrayList<Message>(0));
  }

  public TaskHeartbeat(final String id,
                       final TaskState state,
                       final Collection<TaskStateTransition> stateTransitions,
                       final List<Message> messages) {
    this(id, state, stateTransitions, Optional.<ErrorMessage>empty(), messages);

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
   * @return the errorMessage, if there was one.
   */
  public Optional<ErrorMessage> getErrorMessage() {
    return errorMessage;
  }

  /**
   * @return the messages sent by the Task, if any.
   */
  public List<Message> getMessages() {
    return messages;
  }
}
