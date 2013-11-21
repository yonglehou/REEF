package com.microsoft.reef.simple;

public class AsyncTaskRequest {
  public AsyncTaskRequest(Class<? extends ApplicationTask> clazz, String args) {
    this.clazz = clazz;
    this.args = args;
  }
  public final Class<? extends ApplicationTask> clazz;
  public final String args;

}
