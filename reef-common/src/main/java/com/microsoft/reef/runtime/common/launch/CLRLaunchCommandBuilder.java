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
package com.microsoft.reef.runtime.common.launch;

import org.apache.commons.lang.StringUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A builder for the command line to launch a CLR Evaluator.
 */
public class CLRLaunchCommandBuilder implements LaunchCommandBuilder {
  private static final Logger LOG = Logger.getLogger(CLRLaunchCommandBuilder.class.getName());
  private static final String EVALUATOR_PATH = "reef/global/Microsoft.Reef.Evaluator.exe";


  private String standardErrPath = null;
  private String standardOutPath = null;
  private String errorHandlerRID = null;
  private String launchID = null;
  private int megaBytes = 0;
  private String evaluatorConfigurationPath = null;

  @Override
  public List<String> build() {
    final List<String> result = new LinkedList<>();
    result.add(EVALUATOR_PATH);
    result.add(errorHandlerRID);
    result.add(evaluatorConfigurationPath);
    if ((null != this.standardOutPath) && (!standardOutPath.isEmpty())) {
      result.add(">" + this.standardOutPath);
    }
    if ((null != this.standardErrPath) && (!standardErrPath.isEmpty())) {
      result.add("2>" + this.standardErrPath);
    }
    LOG.log(Level.FINE, "Launch Exe: {0}", StringUtils.join(result, ' '));
    return result;
  }

  @Override
  public CLRLaunchCommandBuilder setErrorHandlerRID(final String errorHandlerRID) {
    this.errorHandlerRID = errorHandlerRID;
    return this;
  }

  @Override
  public CLRLaunchCommandBuilder setLaunchID(final String launchID) {
    this.launchID = launchID;
    return this;
  }

  @Override
  public CLRLaunchCommandBuilder setMemory(final int megaBytes) {
    this.megaBytes = megaBytes;
    return this;
  }

  @Override
  public CLRLaunchCommandBuilder setConfigurationFileName(final String configurationFileName) {
    this.evaluatorConfigurationPath = configurationFileName;
    return this;
  }

  @Override
  public CLRLaunchCommandBuilder setStandardOut(final String standardOut) {
    this.standardOutPath = standardOut;
    return this;
  }

  @Override
  public CLRLaunchCommandBuilder setStandardErr(final String standardErr) {
    this.standardErrPath = standardErr;
    return this;
  }
}
