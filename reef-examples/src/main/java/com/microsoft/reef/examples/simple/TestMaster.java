package com.microsoft.reef.examples.simple;

import javax.inject.Inject;

import com.microsoft.reef.driver.activity.FailedActivity;
import com.microsoft.reef.simple.ApplicationMaster;
import com.microsoft.reef.simple.ApplicationTask;

public class TestMaster extends ApplicationMaster {

  int failedTasks;
  
  @Inject TestMaster() { }
  
  private static class Runme implements ApplicationTask {

    @Inject Runme() {}
    @Override
    public void run(String taskArgs) throws Exception {
      if(Math.random() > 0.01)
      {
        throw new IllegalStateException("Failed because I felt like it!");
      }
      System.out.println(taskArgs);
      Runtime.getRuntime().exec("c:\\windows\\notepad.exe");
    }
    
  }
  private static class RunmeToo implements ApplicationTask {

    @Inject RunmeToo() {}
    @Override
    public void run(String taskArgs) throws Exception {
      if(Math.random() > 0.9)
      {
        throw new IllegalStateException("Failed because I felt like it!");
      }
      System.out.println(taskArgs);
      Runtime.getRuntime().exec("c:\\windows\\system32\\calc.exe");
    }
    
  }
  
  @Override
  public void start(String appArgs) throws InterruptedException {
    out.println("Queuing three tasks"); out.flush();
    fork(Runme.class, appArgs + " 1");
    fork(Runme.class, appArgs + " 2");
    fork(Runme.class, appArgs + " 3");

    join();
    out.println("Tasks done running. (" + failedTasks + " failures)  now for something completely different!"); out.flush();
    fork(RunmeToo.class, appArgs);
    join();
    out.println("join 2 complete!"); out.flush();
  }
  
  @Override
  public synchronized void onTaskFailed(FailedActivity fa) {
    failedTasks++;
//    out.println("Failed task: " + fa.getReason()); out.flush();
  }
  @Override
  public synchronized void onShutdown() {
    out.println("onShutdown called!");
  }
}
