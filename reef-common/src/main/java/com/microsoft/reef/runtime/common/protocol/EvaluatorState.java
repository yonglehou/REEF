package com.microsoft.reef.runtime.common.protocol;

/**
 * The states an Evaluator can be in.
 */
public enum EvaluatorState {
  INIT,
  RUNNING,
  DONE,
  SUSPEND,
  FAILED,
  KILLED
}
