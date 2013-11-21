package com.microsoft.reef.simple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.driver.activity.ActivityConfiguration;
import com.microsoft.reef.driver.activity.CompletedActivity;
import com.microsoft.reef.driver.activity.FailedActivity;
import com.microsoft.reef.driver.contexts.ActiveContext;
import com.microsoft.reef.driver.contexts.ContextConfiguration;
import com.microsoft.reef.driver.contexts.FailedContext;
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

  private final EvaluatorRequestor requestor;
  private final ApplicationMaster appClass;
  private final String appArgs;
  private final int numContainers;
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
      @Parameter(Client.AppClass.class) ApplicationMaster appClass,
      @Parameter(Client.AppArgs.class) String appArgs,
      @Parameter(Client.NumContainers.class) int numContainers,
      @Parameter(Client.ContainerMemory.class) int containerMemory) {
    this.requestor = requestor;
    this.appClass = appClass;
    this.appArgs = appArgs;
    this.numContainers = numContainers;
    this.containerMemory = containerMemory;
    
    appClass.setDriver(this);
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
      appClass.start(appArgs);
      appMasterDone = true;
    }
  }

  
  private synchronized final void executeTasks() {
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
              .build());
        activityConfiguration.bindNamedParameter(Client.TaskClass.class, task.clazz);
        activityConfiguration.bindNamedParameter(Client.TaskArgs.class, task.args);
        context.submitActivity(activityConfiguration.build());
      } catch (final BindException ex) {
        throw new RuntimeException("Unable to setup Activity or Context configuration.", ex);
      }
    }
    if(queuedTasks.isEmpty() && appMasterDone) {
      for(ActiveContext eval : idleEvaluators) {
        eval.close();
      }
      idleEvaluators.clear();
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
  private void onFailedContext(ActiveContext context) {
    synchronized(SimpleDriver.this) {
      queuedTasks.add(runningTasks.get(context));
      idleEvaluators.add(context);
      runningTasks.remove(context);
    }
    executeTasks();
  }
  private void onFailedActivity(FailedActivity failedActivity) {
    LOG.log(Level.WARNING, failedActivity + " failed: " + failedActivity.getReason().get());
    onFailedContext(failedActivity.getActiveContext().get());
  }
  final class EvaluatorFailedHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(FailedEvaluator failedEvaluator) {
      if(failedEvaluator.getFailedActivity().isPresent()) {
        onFailedActivity(failedEvaluator.getFailedActivity().get());
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
    }
    
  }
}
