package com.microsoft.reef.simple;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.activity.ActivityMessageSource;
import com.microsoft.reef.driver.activity.ActivityConfiguration;
import com.microsoft.reef.driver.activity.ActivityMessage;
import com.microsoft.reef.driver.activity.CompletedActivity;
import com.microsoft.reef.driver.activity.FailedActivity;
import com.microsoft.reef.driver.activity.ActivityConfigurationOptions.ActivityMessageSources;
import com.microsoft.reef.driver.client.JobMessageObserver;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;

@Unit
public class SimpleDriver {

  private static final Logger LOG = Logger.getLogger(SimpleDriver.class.getName());

  private Thread amThread;
  
  private final EvaluatorRequestor requestor;
  private final JobMessageObserver client;
  private final ApplicationMaster appMaster;
  private final String appArgs;
  private final int numContainers;
  @SuppressWarnings("unused")
  private final int containerMemory;
  private boolean appMasterDone = false;

  private final Set<ActiveContext> idleEvaluators = new HashSet<>();
  final Set<AsyncTaskRequest> queuedTasks = new HashSet<>();
  private final Map<ActiveContext, AsyncTaskRequest> runningTasks = new HashMap<>();

  /**
   * Job driver constructor - instantiated via TANG.
   *
   * @param requestor evaluator requestor object used to create new evaluator containers.
   */
  @Inject
  public SimpleDriver(final EvaluatorRequestor requestor,
      final JobMessageObserver client,
      @Parameter(Client.AppClass.class) ApplicationMaster appMaster,
      @Parameter(Client.AppArgs.class) String appArgs,
      @Parameter(Client.NumContainers.class) int numContainers,
      @Parameter(Client.ContainerMemory.class) int containerMemory) {
    this.requestor = requestor;
    this.client = client;
    this.appMaster = appMaster;
    this.appArgs = appArgs;
    this.numContainers = numContainers;
    this.containerMemory = containerMemory;
    appMaster.setDriver(this);
  }

  /**
   * Handles the StartTime event: Request evaluators, spawn ApplicationMaster thread.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      requestor.submit(EvaluatorRequest.newBuilder()
          // XXX fix reef size API.
          .setNumber(numContainers).setSize(/*containerMemory*/EvaluatorRequest.Size.XLARGE).build());
      LOG.log(Level.INFO, "StartTime: ", startTime);
      amThread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            appMaster.start(appArgs);
            appMasterDone = true;
            executeTasks();
          } catch(Exception e) {
            if(!(e instanceof RuntimeException)) {
              throw new RuntimeException(e);
            } else {
              throw (RuntimeException)e;
            }
          }
        }
      });
      amThread.start();
    }
  }

  protected final PrintStream out = new PrintStream(/*new BufferedOutputStream(*/new OutputStream() {
    // XXX this is grossly non-performant!
    List<Byte> buf = new ArrayList<Byte>();
    @Override
    public synchronized void write(int arg0) throws IOException {
      buf.add((byte)arg0);
    }
    @Override
    public synchronized void write(byte[] arg0) throws IOException {
      for(int i = 0; i < arg0.length; i++) {
        buf.add(arg0[i]);
      }
    }
    @Override
    public synchronized void flush() throws IOException {
      if(buf.size() != 0) {
        byte[] b = new byte[buf.size()];
        for(int i = 0; i < b.length; i++) {
          b[i] = buf.get(i);
        }
        buf.clear();
        client.onNext(b);
      }
    }
    @Override
    public synchronized void close() throws IOException {
      flush();
    }
  }/*, 64*1024)*/);
  
  synchronized final void executeTasks() {
    while((!idleEvaluators.isEmpty()) && (!queuedTasks.isEmpty())) {
      try {
        ActiveContext context = idleEvaluators.iterator().next();
        idleEvaluators.remove(context);
        AsyncTaskRequest task = queuedTasks.iterator().next();
        queuedTasks.remove(task);
        runningTasks.put(context, task);
        final JavaConfigurationBuilder activityConfiguration = Tang.Factory.getTang().newConfigurationBuilder(
            ActivityConfiguration.CONF
              .set(ActivityConfiguration.IDENTIFIER, task.clazz.getName())
              .set(ActivityConfiguration.ACTIVITY, SimpleActivity.class)
              .set(ActivityConfiguration.ON_SEND_MESSAGE, SimpleActivity.ActivityMessageSource.class)
              .set(ActivityConfiguration.ON_MESSAGE, SimpleActivity.DriverMessageHandler.class)
              .build());
        activityConfiguration.bindNamedParameter(Client.TaskClass.class, task.clazz);
        activityConfiguration.bindNamedParameter(Client.TaskArgs.class, new String(task.args));
        context.submitActivity(activityConfiguration.build());
      } catch (final BindException ex) {
        throw new RuntimeException("Unable to setup Activity or Context configuration.", ex);
      }
    }
    if(queuedTasks.isEmpty() && runningTasks.isEmpty()) {
      notifyAll();
      if(appMasterDone) {
        out.println("closing evaluators");
        for(ActiveContext eval : idleEvaluators) {
          eval.close();
        }
        idleEvaluators.clear();
        appMaster.onShutdown();
        out.flush();
        if(Thread.currentThread() != amThread) {
          try {
            while(amThread.isAlive()) {  // XXX executeTasks() generally gets called last in the amThread, so there's no good way to join().
              out.println("joining am thread");
              for(StackTraceElement e: amThread.getStackTrace()) {
                out.println(e);
              }
              out.flush();
              amThread.join(1000);
            }
          } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while joining app master thread, which should already be shut down!");
          }
        }
      }
    }
  }
  synchronized public Future<byte[]> fork(Class<? extends ApplicationTask> clazz, byte[] arg) {
    Future<byte[]> fut = new Future<byte[]>() {

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public byte[] get() throws InterruptedException, ExecutionException {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public byte[] get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public boolean isCancelled() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean isDone() {
        // TODO Auto-generated method stub
        return false;
      }
      
    };
    queuedTasks.add(new AsyncTaskRequest(clazz, arg, fut));
    executeTasks();
    return fut;
  }
  synchronized final void join() throws InterruptedException {
    // we'll wait until nothing is running or runnable.
    while(!(queuedTasks.isEmpty() && runningTasks.isEmpty())) {
      wait(); 
    }
  }
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "An evaluator has been allocated: {0}", allocatedEvaluator);
      final Configuration contextConfiguration;
      try {
        contextConfiguration = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, "Empty context")
          .build();
      } catch(BindException e) {
        throw new RuntimeException("Could not setup Context configuration", e);
      }
      allocatedEvaluator.submitContext(contextConfiguration);

    }
  }
  private synchronized void onFailedContext(ActiveContext context) {
    if(!queuedTasks.add(runningTasks.get(context))) {
      throw new IllegalStateException("Add to queuedTasks failed (task was already there!)" + runningTasks.get(context) + " running in failed " + context);
    }
    if(!idleEvaluators.add(context)) {
      throw new IllegalStateException("Add to idleEvaluators failed (idle evaluator was already there!)" + context);
    }
    if(null == runningTasks.remove(context)) {
      throw new IllegalStateException("removing running task failed (it went from running to not while I held a lock)" + context);
    }
    executeTasks();
  }
  private void onFailedActivity(FailedActivity failedActivity) {
    LOG.log(Level.WARNING, failedActivity + " failed: " + failedActivity.getReason().get());
    onFailedContext(failedActivity.getActiveContext().get());
    appMaster.onTaskFailed(failedActivity);
  }
  final class EvaluatorFailedHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(FailedEvaluator failedEvaluator) {
      if(failedEvaluator.getFailedActivity().isPresent()) {
        onFailedActivity(failedEvaluator.getFailedActivity().get());
      }
      appMaster.onContainerFailed(failedEvaluator);
    }
  }
  final class ActivityMessageHandler implements EventHandler<ActivityMessage> {

    @Override
    public void onNext(ActivityMessage arg0) {
      try {
        out.write(arg0.get());
      } catch(IOException e) {
        e.printStackTrace();
      }
    }
    
  }
  final class ContextFailedHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(FailedContext failedContext) {
      // XXX no good way to get to the evaluator.  leak it for now.
      // The failed activity will be recovered in a separate call to the
      // failed activity handler.
    }
  }
  final class ActivityCompletedHandler implements EventHandler<CompletedActivity> {
    @Override
    public void onNext(CompletedActivity completedActivity) {
      synchronized(SimpleDriver.this) {
        idleEvaluators.add(completedActivity.getActiveContext());
        runningTasks.remove(completedActivity.getActiveContext());
        appMaster.onTaskCompleted(completedActivity);
      }
      executeTasks();
    }
  }
  final class ActivityFailedHandler implements EventHandler<FailedActivity> {
    @Override
    public void onNext(FailedActivity failedActivity) {
      onFailedActivity(failedActivity);
    }
  }
  final class ContextActiveHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(ActiveContext activeContext) {
      synchronized(SimpleDriver.this) {
        idleEvaluators.add(activeContext);
      }
      executeTasks();
      appMaster.onContainerStarted(activeContext);
    }
    
  }
}
