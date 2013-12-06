package com.microsoft.reef.simple;

public class WrappedThrowable extends Exception {
  final byte[] msg;
  final Throwable throwable;
  private static final long serialVersionUID = 1L;
  public WrappedThrowable(byte[] msg, Throwable throwable) {
    this.msg = msg;
    this.throwable = throwable;
  }

}
