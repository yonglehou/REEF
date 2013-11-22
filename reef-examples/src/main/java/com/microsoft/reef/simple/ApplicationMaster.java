package com.microsoft.reef.simple;

import java.io.PrintStream;
import java.util.logging.Logger;

import com.microsoft.reef.driver.activity.CompletedActivity;
import com.microsoft.reef.driver.activity.FailedActivity;
import com.microsoft.reef.driver.contexts.ActiveContext;
import com.microsoft.reef.driver.contexts.ClosedContext;
import com.microsoft.reef.driver.contexts.ContextMessage;
import com.microsoft.reef.driver.contexts.FailedContext;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;

public abstract class ApplicationMaster {
  @SuppressWarnings("unused")
  private static final Logger LOG = Logger.getLogger(ApplicationMaster.class.getName());

  private SimpleDriver driver;
  protected PrintStream out;
  public abstract void start(String appArgs);
  public void setDriver(SimpleDriver driver) {
    this.driver = driver;
    this.out = driver.out;
  }
  public void queueTaskForExecution(AsyncTaskRequest task) {
    driver.queuedTasks.add(task);
  }
  public void onTaskCompleted(CompletedActivity completedActivity) { }
  public void onTaskFailed(FailedActivity failedActivity) { }
  //public void onApplicationError(XXX unsupported) { }
  public void onShutdown() { }
  public void onContainerStarted(ActiveContext context) { }
  /**
   * Note, this means that the simple drivers' context failed to instantiate, but the
   * evaluator is still OK.
   * 
   * This "can't happen", but SimpleDriver will respond by restarting another one.
   */
  public void onContainerRecovering(FailedContext context) { }
  /**
   * This means a container failed before or after running an activity.  If
   * the an application task failed, then onTaskFailed will be called instead.
   * 
   * When fault tolerance is enabled, is generally OK to ignore these events,
   * since SimpleDriver will transparently mask them by asking for another
   * container.
   */
  public void onContainerFailed(FailedEvaluator context) { }
  public void onContainerStopped(ClosedContext context)  { }
  public void onContainerMessage(ContextMessage context) { }
  
}
