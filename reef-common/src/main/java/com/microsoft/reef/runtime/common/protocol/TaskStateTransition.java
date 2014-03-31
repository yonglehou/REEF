package com.microsoft.reef.runtime.common.protocol;

/**
 * A task state transition.
 */
public final class TaskStateTransition implements Comparable<TaskStateTransition> {
  /**
   * The evaluator heartbeat in which the transition happened.
   */
  private final int sequenceNumber;

  /**
   * The old state.
   */
  private final TaskState from;
  /**
   * The new state.
   */
  private final TaskState to;

  /**
   * @param sequenceNumber The evaluator heartbeat in which the transition happened.
   * @param from           The old state.
   * @param to             The new state.
   */
  public TaskStateTransition(final int sequenceNumber, final TaskState from, final TaskState to) {
    this.sequenceNumber = sequenceNumber;
    this.from = from;
    this.to = to;
  }

  /**
   * @return The evaluator heartbeat in which the transition happened.
   */
  public int getSequenceNumber() {
    return sequenceNumber;
  }

  /**
   * @return The old state.
   */
  public TaskState getFrom() {
    return from;
  }

  /**
   * @return The new state.
   */
  public TaskState getTo() {
    return to;
  }

  /**
   * @return true, if the transition encoded is legal. False otherwise.
   */
  public boolean isLegal() {
    switch (this.from) {
      case INIT:
        switch (this.to) {
          case INIT:
            return false;
          default:
            return true;
        }
      case RUNNING:
        switch (this.to) {
          case INIT:
          case RUNNING:
            return false;
          default:
            return true;
        }
      case SUSPEND:
      case FAILED:
      case DONE:
        return false;
      default:
        return false;
    }
  }

  @Override
  public int compareTo(final TaskStateTransition other) {
    return Integer.compare(this.sequenceNumber, other.sequenceNumber);
  }

  @Override
  public String toString() {
    return "TaskStateTransition{" +
        "sequenceNumber=" + sequenceNumber +
        ", from=" + from +
        ", to=" + to +
        '}';
  }
}
