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
package com.microsoft.reef.examples.nggroup.broadcast;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.evaluator.context.parameters.ContextIdentifier;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ControlMessageBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Dimensions;
import com.microsoft.reef.examples.nggroup.broadcast.parameters.ModelBroadcaster;
import com.microsoft.reef.examples.nggroup.broadcast.parameters.ModelReceiveAckReducer;
import com.microsoft.reef.examples.nggroup.broadcast.parameters.NumberOfReceivers;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupDriver;
import com.microsoft.reef.io.network.nggroup.api.GroupCommDriver;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.reef.io.serialization.SerializableCodec;
import com.microsoft.reef.poison.PoisonedConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationSerializer;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;

/**
 *
 */
@DriverSide
@Unit
public class BroadcastDriver {
  private static final Logger LOG = Logger.getLogger(BroadcastDriver.class.getName());

  private final GroupCommDriver groupCommDriver;

  private final CommunicationGroupDriver allCommGroup;

  private final AtomicBoolean masterSubmitted = new AtomicBoolean(false);

  private final AtomicInteger slaveIds = new AtomicInteger(0);

  private String groupCommConfiguredMasterId;

  private final ConfigurationSerializer confSerializer;

  private final int dimensions;

  private final EvaluatorRequestor requestor;

  private final int numberOfReceivers;

  private final AtomicInteger numberOfAllocatedEvaluators;

  private final AtomicInteger failureSet = new AtomicInteger(0);

  @Inject
  public BroadcastDriver(
      final EvaluatorRequestor requestor,
      final GroupCommDriver groupCommDriver,
      final ConfigurationSerializer confSerializer,
      @Parameter(Dimensions.class) final int dimensions,
      @Parameter(NumberOfReceivers.class) final int numberOfReceivers){
    this.requestor = requestor;
    this.groupCommDriver = groupCommDriver;
    this.confSerializer = confSerializer;
    this.dimensions = dimensions;
    this.numberOfReceivers = numberOfReceivers;
    this.numberOfAllocatedEvaluators = new AtomicInteger(numberOfReceivers + 1);

    this.allCommGroup = this.groupCommDriver.newCommunicationGroup(AllCommunicationGroup.class, numberOfReceivers + 1);
    LOG.info("Obtained all communication group");

    final Codec<ControlMessages> controlMsgCodec = new SerializableCodec<>() ;
    final Codec<Vector> modelCodec = new SerializableCodec<>();
    final Codec<Boolean> modelReceiveAckCodec = new SerializableCodec<>();

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
      .addReduce(ModelReceiveAckReducer.class,
          ReduceOperatorSpec
            .newBuilder()
            .setReceiverId("MasterTask")
            .setDataCodecClass(modelReceiveAckCodec.getClass())
            .setReduceFunctionClass(ModelReceiveAckReduceFunction.class)
            .build()
            )
      .finalise();

    LOG.info("Added operators to allCommGroup");
  }

  /**
   * Handles the StartTime event: Request numOfReceivers Evaluators.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      final int numEvals = BroadcastDriver.this.numberOfReceivers + 1;
      LOG.info("Requesting " + numEvals + " evaluators");
      BroadcastDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(numEvals)
          .setMemory(2048)
          .build());
      LOG.log(Level.INFO, "Requested Evaluators.");
    }
  }

  /**
   * Handles AllocatedEvaluator: Submits a context with an id
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Submitting an id context to AllocatedEvaluator: {0}", allocatedEvaluator);
      try {
        final Configuration contextConfiguration = ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, "BroadcastContext-" + BroadcastDriver.this.numberOfAllocatedEvaluators.getAndDecrement())
            .build();
        allocatedEvaluator.submitContext(contextConfiguration);
      } catch (final BindException ex) {
        throw new RuntimeException("Unable to setup Task or Context configuration.", ex);
      }
    }
  }

  public class FailedTaskHandler implements EventHandler<FailedTask> {

    @Override
    public void onNext(final FailedTask failedTask) {
      LOG.info("Got failed Task " + failedTask.getId());
      final ActiveContext activeContext = failedTask.getActiveContext().get();
      final Configuration partialTaskConf = Tang.Factory.getTang()
          .newConfigurationBuilder(
              TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, failedTask.getId())
              .set(TaskConfiguration.TASK, SlaveTask.class)
              .build(),
              PoisonedConfiguration.TASK_CONF
              .set(PoisonedConfiguration.CRASH_PROBABILITY, "0")
              .set(PoisonedConfiguration.CRASH_TIMEOUT, "1")
              .build()
              )
          .bindNamedParameter(Dimensions.class, Integer.toString(dimensions))
          .build();
      //Do not add the task back
      //allCommGroup.addTask(partialTaskConf);
      final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
      LOG.info("Submitting SlaveTask conf");
      LOG.info(confSerializer.toString(taskConf));
      activeContext.submitTask(taskConf);
    }
  }

  public class ContextActiveHandler implements EventHandler<ActiveContext> {

    private final AtomicBoolean storeMasterId = new AtomicBoolean(false);

    @Override
    public void onNext(final ActiveContext activeContext) {
      LOG.info("Got active context-" + activeContext.getId());
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
                  /*,PoisonedConfiguration.TASK_CONF
                  .set(PoisonedConfiguration.CRASH_PROBABILITY, "0.4")
                  .set(PoisonedConfiguration.CRASH_TIMEOUT, "1")
                  .build()*/
                  )
              .bindNamedParameter(Dimensions.class, Integer.toString(dimensions))
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
        if(storeMasterId.compareAndSet(false, true)){
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

}
