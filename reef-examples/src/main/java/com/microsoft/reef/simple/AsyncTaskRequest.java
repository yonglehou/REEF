package com.microsoft.reef.simple;

import java.util.concurrent.Future;

public class AsyncTaskRequest {
  public AsyncTaskRequest(Class<? extends ApplicationTask> clazz, byte[] args, Future<byte[]> fut) {
    this.clazz = clazz;
    this.args = args;
    this.fut = fut;
  }
  public final Class<? extends ApplicationTask> clazz;
  public final byte[] args;
  public final Future<byte[]> fut;

}
