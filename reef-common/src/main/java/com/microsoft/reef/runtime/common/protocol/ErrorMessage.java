package com.microsoft.reef.runtime.common.protocol;

import com.microsoft.reef.driver.evaluator.EvaluatorType;
import com.microsoft.reef.util.Optional;

/**
 * Represents errors when they are sent across the wire.
 */
public final class ErrorMessage {
  /**
   * The short description of the error.
   */
  private final String shortMessage;
  /**
   * The long description of the error.
   */
  private final Optional<String> longMessage;
  /**
   * The type (JVM, CLR) of the environment where the error occured.
   */
  private final EvaluatorType type;
  /**
   * The exception, if any
   */
  private final Optional<byte[]> serializedException;

  /**
   * @param shortMessage        The short description of the error.
   * @param longMessage         The long description of the error.
   * @param type                The type (JVM, CLR) of the environment where the error occured.
   * @param serializedException The exception, if any.
   */
  public ErrorMessage(final String shortMessage,
                      final Optional<String> longMessage,
                      final EvaluatorType type,
                      final Optional<byte[]> serializedException) {
    this.shortMessage = shortMessage;
    this.longMessage = longMessage;
    this.type = type;
    this.serializedException = serializedException;
  }

  /**
   * @param shortMessage        The short description of the error.
   * @param longMessage         The long description of the error.
   * @param type                The type (JVM, CLR) of the environment where the error occured.
   * @param serializedException The exception.
   */
  public ErrorMessage(final String shortMessage,
                      final Optional<String> longMessage,
                      final EvaluatorType type,
                      final byte[] serializedException) {
    this(shortMessage, longMessage, type, Optional.of(serializedException));
  }

  /**
   * @param shortMessage The short description of the error.
   * @param longMessage  The long description of the error.
   * @param type         The type (JVM, CLR) of the environment where the error occured.
   */
  public ErrorMessage(final String shortMessage,
                      final Optional<String> longMessage,
                      final EvaluatorType type) {
    this(shortMessage, longMessage, type, Optional.<byte[]>empty());
  }

  /**
   * @return The short description of the error.
   */
  public String getShortMessage() {
    return shortMessage;
  }

  /**
   * @return The long description of the error.
   */
  public Optional<String> getLongMessage() {
    return longMessage;
  }

  /**
   * @return The type (JVM, CLR) of the environment where the error occured.
   */
  public EvaluatorType getType() {
    return type;
  }

  /**
   * @return The exception, if any.
   */
  public Optional<byte[]> getSerializedException() {
    return serializedException;
  }

  @Override
  public String toString() {
    return "ErrorMessage{" +
        "shortMessage='" + shortMessage + '\'' +
        ", longMessage='" + longMessage + '\'' +
        ", type=" + type +
        '}';
  }
}
