package com.microsoft.reef.runtime.common.protocol;

/**
 * Represents an Evaluator state transition.
 */
public final class EvaluatorStateTransition implements Comparable<EvaluatorStateTransition> {
  private final int sequenceNumber;
  private final EvaluatorState from;
  private final EvaluatorState to;

  /**
   * @param sequenceNumber at the end of which the evaluator is in state to.
   * @param from           the old state.
   * @param to             the new state.
   */
  public EvaluatorStateTransition(final int sequenceNumber, final EvaluatorState from, final EvaluatorState to) {
    this.sequenceNumber = sequenceNumber;
    this.from = from;
    this.to = to;
  }

  public int getSequenceNumber() {
    return sequenceNumber;
  }

  public EvaluatorState getFrom() {
    return from;
  }

  public EvaluatorState getTo() {
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
  public int compareTo(final EvaluatorStateTransition other) {
    return Integer.compare(this.sequenceNumber, other.sequenceNumber);
  }

  @Override
  public String toString() {
    return "EvaluatorStateTransition{" +
        "sequenceNumber=" + sequenceNumber +
        ", from=" + from +
        ", to=" + to +
        '}';
  }
}
