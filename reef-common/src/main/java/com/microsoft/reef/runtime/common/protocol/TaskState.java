package com.microsoft.reef.runtime.common.protocol;

/**
 * The state of a task.
 */
public enum TaskState {
  INIT,
  RUNNING,
  DONE,
  SUSPEND,
  FAILED,
  KILLED
}
