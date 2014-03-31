package com.microsoft.reef.runtime.common.protocol;

/**
 * A message sent by a Task or Context.
 */
public final class Message {

  /**
   * The identifier of the source sending this message.
   */
  private final String sourceIdentifier;
  /**
   * The message.
   */
  private final byte[] message;

  /**
   * @param sourceIdentifier The identifier of the source sending this message.
   * @param message          The message.
   */
  public Message(final String sourceIdentifier, final byte[] message) {
    this.sourceIdentifier = sourceIdentifier;
    this.message = message;
  }

  /**
   * @return The identifier of the source sending this message.
   */
  public String getSourceIdentifier() {
    return sourceIdentifier;
  }

  /**
   * @return The message.
   */
  public byte[] getMessage() {
    return message;
  }

  @Override
  public String toString() {
    return "Message{" +
        "sourceIdentifier='" + sourceIdentifier + '\'' +
        '}';
  }
}
