package com.microsoft.reef.simple;

import java.util.logging.Logger;

public abstract class ApplicationMaster {
  private static final Logger LOG = Logger.getLogger(ApplicationMaster.class.getName());

  private SimpleDriver driver;
  public abstract void start(String appArgs);
  public void setDriver(SimpleDriver driver) {
    this.driver = driver;
  }
  public void queueTaskForExecution(AsyncTaskRequest task) {
    driver.queuedTasks.add(task);
  }

}
