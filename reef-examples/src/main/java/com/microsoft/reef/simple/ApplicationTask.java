package com.microsoft.reef.simple;

import java.io.PrintStream;

import com.microsoft.reef.activity.ActivityMessage;
import com.microsoft.reef.activity.events.DriverMessage;

public abstract class ApplicationTask {
  protected void init(PrintStream out) {
    this.out = out;
  }
  protected PrintStream out;
  
  public abstract void run(String appArgs) throws Exception;

  public ActivityMessage getMessageForDriver() {
    return null;
  }

  public void onDriverMessageRecieved(DriverMessage arg0) {
  }
}
