package com.microsoft.reef.simple;

import javax.inject.Inject;

import com.microsoft.reef.activity.Activity;
import com.microsoft.tang.annotations.Parameter;

public class SimpleActivity implements Activity {
  private final ApplicationTask task;
  private final String taskArgs;
  @Inject
  SimpleActivity(
      @Parameter(Client.TaskClass.class) ApplicationTask task,
      @Parameter(Client.TaskArgs.class) String taskArgs) {
    this.task = task;
    this.taskArgs = taskArgs;
  }

  @Override
  public final byte[] call(final byte[] memento) throws Exception {
    task.run(taskArgs);
    return null;
  }

}
