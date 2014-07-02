/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.examples.nggroup.bgd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.evaluator.context.parameters.ContextIdentifier;
import com.microsoft.reef.examples.nggroup.bgd.data.parser.Parser;
import com.microsoft.reef.examples.nggroup.bgd.data.parser.SVMLightParser;
import com.microsoft.reef.examples.nggroup.bgd.loss.LogisticLossFunction;
import com.microsoft.reef.examples.nggroup.bgd.loss.LossFunction;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ControlMessageBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.DescentDirectionBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Dimensions;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Eps;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Iterations;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Lambda;
import com.microsoft.reef.examples.nggroup.bgd.parameters.LineSearchEvaluationsReducer;
import com.microsoft.reef.examples.nggroup.bgd.parameters.LossAndGradientReducer;
import com.microsoft.reef.examples.nggroup.bgd.parameters.MinEtaBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ModelAndDescentDirectionBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ModelBroadcaster;
import com.microsoft.reef.io.data.loading.api.DataLoadingService;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupDriver;
import com.microsoft.reef.io.network.nggroup.api.GroupCommDriver;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import com.microsoft.reef.io.network.util.Utils.Pair;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.reef.io.serialization.SerializableCodec;
import com.microsoft.reef.poison.PoisonedConfiguration;
import com.microsoft.reef.runtime.common.driver.DriverStatusManager;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationSerializer;
import com.microsoft.wake.EventHandler;

/**
 *
 */
@DriverSide
@Unit
public class BGDDriver {
  private static final Logger LOG = Logger.getLogger(BGDDriver.class.getName());

  private final DataLoadingService dataLoadingService;

  private final GroupCommDriver groupCommDriver;

  private final CommunicationGroupDriver allCommGroup;

  private final AtomicBoolean masterSubmitted = new AtomicBoolean(false);

  private final AtomicInteger slaveIds = new AtomicInteger(0);

  private String groupCommConfiguredMasterId;

  private final ConfigurationSerializer confSerializer;

  private final int dimensions;

  private final double lambda;

  private final Map<String, RunningTask> runningTasks = new HashMap<>();

  private final AtomicBoolean jobComplete = new AtomicBoolean(false);

  private final Codec<ArrayList<Double>> lossCodec = new SerializableCodec<ArrayList<Double>>();

  private final double eps;

  private final int iters;

  private final DriverStatusManager driverStatusManager;


  @Inject
  public BGDDriver(
      final DataLoadingService dataLoadingService,
      final GroupCommDriver groupCommDriver,
      final ConfigurationSerializer confSerializer,
      final DriverStatusManager driverStatusManager,
      @Parameter(Dimensions.class) final int dimensions,
      @Parameter(Lambda.class) final double lambda,
      @Parameter(Eps.class) final double eps,
      @Parameter(Iterations.class) final int iters){
    this.dataLoadingService = dataLoadingService;
    this.groupCommDriver = groupCommDriver;
    this.confSerializer = confSerializer;
    this.driverStatusManager = driverStatusManager;
    this.dimensions = dimensions;
    this.lambda = lambda;
    this.eps = eps;
    this.iters = iters;

    this.allCommGroup = this.groupCommDriver.newCommunicationGroup(AllCommunicationGroup.class, dataLoadingService.getNumberOfPartitions() + 1);
    LOG.info("Obtained all communication group");

    final Codec<ControlMessages> controlMsgCodec = new SerializableCodec<>() ;
    final Codec<Vector> modelCodec = new SerializableCodec<>();
    final Codec<Pair<Double,Vector>> lossAndGradientCodec = new SerializableCodec<>();
    final Codec<Pair<Vector,Vector>> modelAndDesDirCodec = new SerializableCodec<>();
    final Codec<Vector> desDirCodec = new SerializableCodec<>();
    final Codec<Vector> lineSearchCodec = new SerializableCodec<>();
    final Codec<Double> minEtaCodec = new SerializableCodec<>();
    final ReduceFunction<Pair<Pair<Double,Integer>,Vector>> lossAndGradientReduceFunction = new LossAndGradientReduceFunction();
    final ReduceFunction<Pair<Vector,Integer>> lineSearchReduceFunction = new LineSearchReduceFunction();
    allCommGroup
      .addBroadcast(ControlMessageBroadcaster.class,
          BroadcastOperatorSpec
            .newBuilder()
            .setSenderId("MasterTask")
            .setDataCodecClass(controlMsgCodec.getClass())
            .build())
      .addBroadcast(ModelBroadcaster.class,
          BroadcastOperatorSpec
            .newBuilder()
            .setSenderId("MasterTask")
            .setDataCodecClass(modelCodec.getClass())
            .build())
      .addReduce(LossAndGradientReducer.class,
          ReduceOperatorSpec
            .newBuilder()
            .setReceiverId("MasterTask")
            .setDataCodecClass(lossAndGradientCodec.getClass())
            .setReduceFunctionClass(lossAndGradientReduceFunction.getClass())
            .build())
      .addBroadcast(ModelAndDescentDirectionBroadcaster.class,
          BroadcastOperatorSpec
          .newBuilder()
          .setSenderId("MasterTask")
          .setDataCodecClass(modelAndDesDirCodec.getClass())
          .build())
      .addBroadcast(DescentDirectionBroadcaster.class,
          BroadcastOperatorSpec
          .newBuilder()
          .setSenderId("MasterTask")
          .setDataCodecClass(desDirCodec.getClass())
          .build())
      .addReduce(LineSearchEvaluationsReducer.class,
          ReduceOperatorSpec
          .newBuilder()
          .setReceiverId("MasterTask")
          .setDataCodecClass(lineSearchCodec.getClass())
          .setReduceFunctionClass(lineSearchReduceFunction.getClass())
          .build())
      .addBroadcast(MinEtaBroadcaster.class,
          BroadcastOperatorSpec
          .newBuilder()
          .setSenderId("MasterTask")
          .setDataCodecClass(minEtaCodec.getClass())
          .build())
      .finalise();

    LOG.info("Added operators to allCommGroup");
  }

  public class ContextCloseHandler implements EventHandler<ClosedContext> {

    @Override
    public void onNext(final ClosedContext closedContext) {
      LOG.info("Got closed context-" + closedContext.getId());
      final ActiveContext parentContext = closedContext.getParentContext();
      if(parentContext!=null){
        LOG.info("Closing parent context-" + parentContext.getId());
        parentContext.close();
      }
    }

  }

  public class TaskCompletedHandler implements EventHandler<CompletedTask>{

    @Override
    public void onNext(final CompletedTask task) {
      LOG.info("Got complete task-" + task.getId());
      final byte[] retVal = task.get();
      if(retVal!=null) {
        final List<Double> losses = BGDDriver.this.lossCodec.decode(retVal);
        for (final Double loss : losses) {
          System.out.println(loss);
        }
      }
      LOG.log(Level.FINEST, "Releasing All Contexts");
      synchronized (runningTasks) {
        final RunningTask rTask = runningTasks.remove(task.getId());
        if(rTask!=null) {
          task.getActiveContext().close();
        }
        if(task.getId().equals("MasterTask")) {
          jobComplete.set(true);
          for(final RunningTask runTask : runningTasks.values()) {
            runTask.getActiveContext().close();
          }
          runningTasks.clear();
          driverStatusManager.onComplete();
        }
      }


//      task.getActiveContext().close();

    }

  }

  public class TaskRunningHandler implements EventHandler<RunningTask> {

    @Override
    public void onNext(final RunningTask runningTask) {
      synchronized (runningTasks) {
        if(!jobComplete.get()) {
          LOG.info("Job has not completed yet. Adding to runningTasks");
          runningTasks.put(runningTask.getId(), runningTask);
        }
        else {
          LOG.info("Job has completed. Not adding to runningTasks. Closing the active context");
          runningTask.getActiveContext().close();
        }
      }
    }

  }

  public class ContextActiveHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext activeContext) {
      LOG.info("Got active context-" + activeContext.getId());
      synchronized (runningTasks) {
        if (jobComplete.get()) {
          LOG.info("Job has completed. Not submitting any task. Closing activecontext");
          activeContext.close();
        }
      }
      /**
       * The active context can be either from
       * data loading service or after network
       * service has loaded contexts. So check
       * if the GroupCommDriver knows if it was
       * configured by one of the communication
       * groups
       */
      if(groupCommDriver.configured(activeContext)){
        if(activeContext.getId().equals(groupCommConfiguredMasterId) && !masterTaskSubmitted()){
          final Configuration partialTaskConf = Tang.Factory.getTang()
              .newConfigurationBuilder(
                  TaskConfiguration.CONF
                  .set(TaskConfiguration.IDENTIFIER, "MasterTask")
                  .set(TaskConfiguration.TASK, MasterTask.class)
                  .build())
               .bindNamedParameter(Dimensions.class, Integer.toString(dimensions))
               .bindNamedParameter(Lambda.class, Double.toString(lambda))
               .bindNamedParameter(Eps.class, Double.toString(eps))
               .bindNamedParameter(Iterations.class, Integer.toString(iters))
               .build();

          allCommGroup.addTask(partialTaskConf);
          final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
          LOG.info("Submitting MasterTask conf");
          LOG.info(confSerializer.toString(taskConf));
          activeContext.submitTask(taskConf);
        }
        else{
          final Configuration partialTaskConf = Tang.Factory.getTang()
              .newConfigurationBuilder(
                  TaskConfiguration.CONF
                  .set(TaskConfiguration.IDENTIFIER, getSlaveId(activeContext))
                  .set(TaskConfiguration.TASK, SlaveTask.class)
                  .build()
                  ,PoisonedConfiguration.TASK_CONF
                  .set(PoisonedConfiguration.CRASH_PROBABILITY, "0.4")
                  .set(PoisonedConfiguration.CRASH_TIMEOUT, "1")
                  .build())
              .bindNamedParameter(Dimensions.class, Integer.toString(dimensions))
              .bindImplementation(Parser.class, SVMLightParser.class)
              .bindImplementation(LossFunction.class, LogisticLossFunction.class)
              .build();
          allCommGroup.addTask(partialTaskConf);
          final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
          LOG.info("Submitting SlaveTask conf");
          LOG.info(confSerializer.toString(taskConf));
          activeContext.submitTask(taskConf);
        }
      }
      else{
        final Configuration contextConf = groupCommDriver.getContextConf();
        final String contextId = contextId(contextConf);
        if(!dataLoadingService.isDataLoadedContext(activeContext)){
          groupCommConfiguredMasterId = contextId;
        }
        LOG.info("Submitting GCContext conf");
        LOG.info(confSerializer.toString(contextConf));

        final Configuration serviceConf = groupCommDriver.getServiceConf();
        LOG.info("Submitting Service conf");
        LOG.info(confSerializer.toString(serviceConf));
        activeContext.submitContextAndService(contextConf, serviceConf);
      }
    }

    /**
     * @param contextConf
     * @return
     */
    private String contextId(final Configuration contextConf) {
      try{
        final Injector injector = Tang.Factory.getTang().newInjector(contextConf);
        return injector.getNamedInstance(ContextIdentifier.class);
      }catch(final InjectionException e){
        throw new RuntimeException("Unable to inject context identifier from context conf", e);
      }
    }

    /**
     * @param activeContext
     * @return
     */
    private String getSlaveId(final ActiveContext activeContext) {
      return "SlaveTask-" + slaveIds.getAndIncrement();
    }

    /**
     * @return
     */
    private boolean masterTaskSubmitted() {
      return !masterSubmitted.compareAndSet(false, true);
    }
  }

  class TaskFailedHandler implements EventHandler<FailedTask> {

    @Override
    public void onNext(final FailedTask failedTask) {
      LOG.info("Got failed Task " + failedTask.getId());

      synchronized (runningTasks) {
        if (jobComplete.get()) {
          LOG.info("Job has completed. Not resubmitting. Closing activecontext");
          final RunningTask rTask = runningTasks.remove(failedTask.getId());
          if(rTask!=null) {
            rTask.getActiveContext().close();
          }
          return;
        }
        else {
          runningTasks.remove(failedTask.getId());
        }
      }
      final ActiveContext activeContext = failedTask.getActiveContext().get();
      final Configuration partialTaskConf = Tang.Factory.getTang()
          .newConfigurationBuilder(
              TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, failedTask.getId())
              .set(TaskConfiguration.TASK, SlaveTask.class)
              .build()
              ,PoisonedConfiguration.TASK_CONF
              .set(PoisonedConfiguration.CRASH_PROBABILITY, "0")
              .set(PoisonedConfiguration.CRASH_TIMEOUT, "1")
              .build())
          .bindNamedParameter(Dimensions.class, Integer.toString(dimensions))
          .bindImplementation(Parser.class, SVMLightParser.class)
          .bindImplementation(LossFunction.class, LogisticLossFunction.class)
          .build();
      //Do not add the task back
      //allCommGroup.addTask(partialTaskConf);
      final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
      LOG.info("Submitting SlaveTask conf");
      LOG.info(confSerializer.toString(taskConf));
      activeContext.submitTask(taskConf);
    }
  }

}
