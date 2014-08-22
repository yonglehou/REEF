package com.microsoft.reef.io.network.group.operators;

public class AllReduceResult<T> {

  private T value = null;

  public AllReduceResult(T val) {
    value = val;
  }

  public boolean isEmpty() {
    return value == null ? true : false;
  }

  public T getValue() {
    return value;
  }
}
