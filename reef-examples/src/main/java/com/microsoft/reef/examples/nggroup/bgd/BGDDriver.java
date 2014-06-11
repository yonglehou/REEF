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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.evaluator.context.parameters.ContextIdentifier;
import com.microsoft.reef.examples.nggroup.bgd.data.parser.Parser;
import com.microsoft.reef.examples.nggroup.bgd.data.parser.SVMLightParser;
import com.microsoft.reef.examples.nggroup.bgd.loss.LogisticLossFunction;
import com.microsoft.reef.examples.nggroup.bgd.loss.LossFunction;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ControlMessageBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Dimensions;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Eps;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Iterations;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Lambda;
import com.microsoft.reef.examples.nggroup.bgd.parameters.LineSearchEvaluationsReducer;
import com.microsoft.reef.examples.nggroup.bgd.parameters.LossAndGradientReducer;
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



  private final Codec<ArrayList<Double>> lossCodec = new SerializableCodec<ArrayList<Double>>();

  private final double eps;

  private final int iters;


  @Inject
  public BGDDriver(
      final DataLoadingService dataLoadingService,
      final GroupCommDriver groupCommDriver,
      final ConfigurationSerializer confSerializer,
      @Parameter(Dimensions.class) final int dimensions,
      @Parameter(Lambda.class) final double lambda,
      @Parameter(Eps.class) final double eps,
      @Parameter(Iterations.class) final int iters){
    this.dataLoadingService = dataLoadingService;
    this.groupCommDriver = groupCommDriver;
    this.confSerializer = confSerializer;
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
    final Codec<Vector> lineSearchCodec = new SerializableCodec<>();
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
      .addReduce(LineSearchEvaluationsReducer.class,
          ReduceOperatorSpec
          .newBuilder()
          .setReceiverId("MasterTask")
          .setDataCodecClass(lineSearchCodec.getClass())
          .setReduceFunctionClass(lineSearchReduceFunction.getClass())
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
    }

  }

  public class ContextActiveHandler implements EventHandler<ActiveContext> {

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

}
