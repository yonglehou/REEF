package com.microsoft.reef.simple;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import com.microsoft.reef.activity.Activity;
import com.microsoft.reef.activity.ActivityMessage;
import com.microsoft.reef.activity.events.DriverMessage;
import com.microsoft.reef.activity.events.SuspendEvent;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;

@Unit
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
  public final class ActivityMessageSource implements com.microsoft.reef.activity.ActivityMessageSource {

    @Override
    public Optional<ActivityMessage> getMessage() {
      byte[] msg = task.getMessageForDriver();
      if(msg == null) {
        return Optional.empty();
      } else {
        return Optional.of(ActivityMessage.from("unknown id", msg));
      }
    }
  }
  public final class DriverMessageHandler implements EventHandler<DriverMessage> {

    @Override
    public void onNext(DriverMessage arg0) {
      task.onDriverMessageRecieved(arg0);
    }
    
  }
  public class SuspendHandler implements EventHandler<SuspendEvent> {

    @Override
    public void onNext(SuspendEvent arg0) {
      task.out.println("Ignoring suspend event!");
    }

  }
}
