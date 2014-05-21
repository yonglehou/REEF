/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.tests.fail.task;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.task.Task;
import com.microsoft.reef.tests.TestDriverLauncher;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

/**
 * Client for the test REEF job that fails on different stages of execution.
 */
public final class Client {

  public static LauncherStatus run(
      final Class<? extends Task> failTaskClass,
      final Configuration runtimeConfig,
      final int timeOut) throws BindException, InjectionException {

    final Configuration driverConfig =
        EnvironmentUtils.addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "Fail_" + failTaskClass.getSimpleName())
            .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, Driver.AllocatedEvaluatorHandler.class)
            .set(DriverConfiguration.ON_TASK_RUNNING, Driver.RunningTaskHandler.class)
            .set(DriverConfiguration.ON_CONTEXT_ACTIVE, Driver.ActiveContextHandler.class)
            .set(DriverConfiguration.ON_DRIVER_STARTED, Driver.StartHandler.class)
            .set(DriverConfiguration.ON_TASK_FAILED, Driver.OnTaskFailed.class)
            .build();

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.addConfiguration(driverConfig);
    cb.bindNamedParameter(Driver.FailTaskName.class, failTaskClass.getName());

    return TestDriverLauncher.getLauncher(runtimeConfig).run(cb.build(), timeOut);
  }
}
