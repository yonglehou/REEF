package com.microsoft.reef.examples.simple;

import javax.inject.Inject;

import com.microsoft.reef.simple.ApplicationMaster;
import com.microsoft.reef.simple.ApplicationTask;
import com.microsoft.reef.simple.AsyncTaskRequest;

public class TestMaster extends ApplicationMaster {

  @Inject TestMaster() { }
  
  private static class Runme implements ApplicationTask {

    @Inject Runme() {}
    @Override
    public void run(String taskArgs) throws Exception {
      if(Math.random() > 0.1)
      {
        throw new IllegalStateException("Failed because I felt like it!");
      }
      System.out.println(taskArgs);
      Runtime.getRuntime().exec("c:\\windows\\notepad.exe");
    }
    
  }
  
  @Override
  public void start(String appArgs) {
    queueTaskForExecution(new AsyncTaskRequest(Runme.class, appArgs + " 1"));
    queueTaskForExecution(new AsyncTaskRequest(Runme.class, appArgs + " 2"));
    queueTaskForExecution(new AsyncTaskRequest(Runme.class, appArgs + " 3"));
  }
}
