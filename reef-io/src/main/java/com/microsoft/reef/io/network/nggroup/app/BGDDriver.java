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
package com.microsoft.reef.io.network.nggroup.app;

import javax.inject.Inject;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.io.data.loading.api.DataLoadingService;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroup;
import com.microsoft.reef.io.network.nggroup.api.GroupCommDriver;
import com.microsoft.reef.io.network.util.Utils.Pair;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;

/**
 * 
 */
@DriverSide
@Unit
public class BGDDriver {
  
  private final DataLoadingService dataLoadingService;
  
  private final GroupCommDriver groupCommDriver;
  
  private final CommunicationGroup allCommGroup; 
  
  @Inject
  public BGDDriver(
      DataLoadingService dataLoadingService,
      GroupCommDriver groupCommDriver){
    this.dataLoadingService = dataLoadingService;
    this.groupCommDriver = groupCommDriver;
    allCommGroup = this.groupCommDriver.newCommunicationGroup("ALL");
    Codec<ControlMessages> controlMsgCodec = null;
    Codec<Vector> modelCodec = null;
    Codec<Pair<Double,Vector>> lossAndGradientCodec = null;
    Codec<Pair<Vector,Vector>> modelAndDesDirCodec = null;
    Codec<Vector> lineSearchCodec = null;
    allCommGroup
      .addBroadcast("ControlMessageBroadcaster", controlMsgCodec)
      .addBroadcast("ModelBroadcaster",modelCodec)
      .addReduce("LossAndGradientReducer", lossAndGradientCodec)
      .addBroadcast("ModelAndDescentDirectionBroadcaster", modelAndDesDirCodec)
      .addReduce("LineSearchEvaluationsReducer", lineSearchCodec)
      .finalize();
  }
  
  public class ContextActiveHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(ActiveContext activeContext) {
      /**
       * The active context can be either from
       * data loading service or after network
       * service has loaded contexts. So check
       * if the GroupCommDriver knows if it was
       * configured by one of the communication
       * groups
       */
      if(groupCommDriver.configured(activeContext)){
        if(allCommGroup.isMaster(activeContext)){
          allCommGroup.submitTask(activeContext,MasterTask.class);
        }
        else{
          allCommGroup.submitTask(activeContext,SlaveTask.class);
        }
      }
      else{
        /**
         * Contexts are added by the CommunicationGroup
         * because each communication group can treat a
         * context differently
         */
        if(dataLoadingService.isDataLoadedContext(activeContext)){
          allCommGroup.addSlaveContext(activeContext);
        }
        else{
          allCommGroup.addMasterContext(activeContext);
        }
        activeContext.submitContextAndService(allCommGroup.getContextConf(activeContext), allCommGroup.getServiceConf(activeContext));
      }
    }
  }

}
