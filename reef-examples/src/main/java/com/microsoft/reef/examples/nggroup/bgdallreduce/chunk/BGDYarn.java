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
package com.microsoft.reef.examples.nggroup.bgdallreduce.chunk;

import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.tang.Configuration;

/**
 * Runs BGD on the YARN runtime.
 */
public class BGDYarn {

  public static void main(final String[] args) throws Exception {
    final BGDClient bgdClient = BGDClient.fromCommandLine(args);
    final Configuration runtimeConfiguration =
      YarnClientConfiguration.CONF.set(YarnClientConfiguration.JVM_HEAP_SLACK,
        0.2).build();
    final LauncherStatus result = bgdClient.run(runtimeConfiguration, System.getProperty("user.name") + "-" + "BGDYarn");
    System.out.println("Result: " + result.toString());
  }
}
