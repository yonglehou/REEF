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

import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.tang.Configuration;

/**
 *
 */
public interface BGDClient {

  /**
   * Runs BGD on the given runtime.
   *
   * @param runtimeConfiguration the runtime to run on.
   * @param jobName              the name of the job on the runtime.
   * @return
   * @throws Exception
   */
  LauncherStatus run(Configuration runtimeConfiguration, String jobName) throws Exception;

  /**
   * Runs BGD on the given runtime.
   *
   * @param runtimeConfiguration the runtime to run on.
   * @param jobName              the name of the job on the runtime.
   * @param timeout              the time after which the job will be killed if not completed, in ms
   * @return
   * @throws Exception
   */
  LauncherStatus run(Configuration runtimeConfiguration, String jobName, int timeout) throws Exception;

}