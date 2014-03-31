package com.microsoft.reef.runtime.common.protocol;

/**
 * Context state transition.
 */
public final class ContextStateTransition implements Comparable<ContextStateTransition> {
  /**
   * The evaluator heartbeat sequence number in which the transition was reported.
   */
  private final int sequenceNumber;
  /**
   * The old state.
   */
  private final ContextState from;
  /**
   * The new state.
   */
  private final ContextState to;

  /**
   * @param sequenceNumber The evaluator heartbeat sequence number in which the transition was reported.
   * @param from           the old state.
   * @param to             the new state.
   */
  public ContextStateTransition(final int sequenceNumber, final ContextState from, final ContextState to) {
    this.sequenceNumber = sequenceNumber;
    this.from = from;
    this.to = to;
  }

  /**
   * @return The evaluator heartbeat sequence number in which the transition was reported.
   */
  public int getSequenceNumber() {
    return sequenceNumber;
  }

  /**
   * @return The old state.
   */
  public ContextState getFrom() {
    return from;
  }

  /**
   * @return The new state.
   */
  public ContextState getTo() {
    return to;
  }

  /**
   * @return true, if the state transition is legal. False, outherwise.
   */
  public boolean isLegal() {
    switch (this.from) {
      case READY:
        switch (this.to) {
          case READY:
            return false;
          default:
            return true;
        }
      case DONE:
      case FAILED:
        return false;
      default:
        return false;
    }
  }

  /**
   * Compares on the sequence number.
   *
   * @param other
   * @return
   */
  @Override
  public int compareTo(final ContextStateTransition other) {
    return Integer.compare(this.sequenceNumber, other.sequenceNumber);
  }

  @Override
  public String toString() {
    return "ContextStateTransition{" +
        "sequenceNumber=" + sequenceNumber +
        ", from=" + from +
        ", to=" + to +
        '}';
  }
}
