package com.microsoft.reef.runtime.common.protocol;

import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.util.Optional;

import java.util.Collection;

/**
 * Heartbeat sent from the Evaluator to the Driver.
 */
@Private
public final class EvaluatorHeartbeat {

  /**
   * The evaluator id.
   */
  private final String id;

  /**
   * The sequence number of this heartbeat.
   */
  private final int sequenceNumber;

  /**
   * The time at the evaluator when this Heartbeat was generated.
   */
  private final long timeStamp;

  /**
   * All state transitions of this Evaluator.
   */
  private final Collection<EvaluatorStateTransition> stateTransitions;

  /**
   * The current state of the Evaluator.
   */
  private final EvaluatorState state;

  /**
   * The error, if there was one.
   */
  private final Optional<ErrorMessage> error;

  /**
   * The heartbeat of the top of the context stack (refers to its parents if there are any).
   */
  private final ContextHeartbeat contextHeartbeat;

  /**
   * The heartbeat of the currently running task, if there is any.
   */
  private final Optional<TaskHeartbeat> taskHeartbeat;

  /**
   * @param id               the id of this Evaluator.
   * @param sequenceNumber   the sequence number of this heartbeat.
   * @param timeStamp        the timestamp of this heartbeat on the Evaluator.
   * @param stateTransitions all state transitions of this Evaluator.
   * @param state            the current state of the Evaluator.
   * @param error            the error thrown by this Evaluator, if any.
   * @param contextHeartbeat the heartbeat of the top Context.
   * @param taskHeartbeat    the heartbeat of the task executing on the Evaluator, if any.
   */
  public EvaluatorHeartbeat(final String id,
                            final int sequenceNumber,
                            final long timeStamp,
                            final Collection<EvaluatorStateTransition> stateTransitions,
                            final EvaluatorState state,
                            final Optional<ErrorMessage> error,
                            final ContextHeartbeat contextHeartbeat,
                            final Optional<TaskHeartbeat> taskHeartbeat) {
    this.id = id;
    this.sequenceNumber = sequenceNumber;
    this.timeStamp = timeStamp;
    this.stateTransitions = stateTransitions;
    this.state = state;
    this.error = error;
    this.contextHeartbeat = contextHeartbeat;
    this.taskHeartbeat = taskHeartbeat;
  }

  public String getId() {
    return id;
  }

  public int getSequenceNumber() {
    return sequenceNumber;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public Collection<EvaluatorStateTransition> getStateTransitions() {
    return stateTransitions;
  }

  public EvaluatorState getState() {
    return state;
  }

  public Optional<ErrorMessage> getError() {
    return error;
  }

  public ContextHeartbeat getContextHeartbeat() {
    return contextHeartbeat;
  }

  public Optional<TaskHeartbeat> getTaskHeartbeat() {
    return taskHeartbeat;
  }
}
