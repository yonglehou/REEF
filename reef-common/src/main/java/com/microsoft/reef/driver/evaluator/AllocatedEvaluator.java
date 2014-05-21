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
package com.microsoft.reef.driver.evaluator;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.driver.ContextAndTaskSubmittable;
import com.microsoft.reef.driver.ContextSubmittable;
import com.microsoft.reef.io.naming.Identifiable;
import com.microsoft.tang.Configuration;

import java.io.File;
import java.io.IOException;

/**
 * Represents an Evaluator that is allocated, but is not running yet.
 */
@Public
@DriverSide
@Provided
public interface AllocatedEvaluator
    extends AutoCloseable, Identifiable, ContextSubmittable, ContextAndTaskSubmittable {

  /**
   * Puts the given file into the working directory of the Evaluator.
   *
   * @param file the file to be copied
   * @throws IOException if the copy fails.
   */
  void addFile(final File file);

  /**
   * Puts the given file into the working directory of the Evaluator and adds it to its classpath.
   *
   * @param file the file to be copied
   * @throws IOException if the copy fails.
   */
  void addLibrary(final File file);

  /**
   * @return the evaluator descriptor of this evaluator.
   */
  EvaluatorDescriptor getEvaluatorDescriptor();

  /**
   * Set the type of Evaluator to be instantiated. Defaults to EvaluatorType.JVM.
   *
   * @param type
   */
  void setType(final EvaluatorType type);

  /**
   * Releases the allocated evaluator back to the resource manager.
   */
  @Override
  void close();

  @Override
  void submitContext(final Configuration contextConfiguration);

  @Override
  void submitContextAndService(final Configuration contextConfiguration,
                               final Configuration serviceConfiguration);

  @Override
  void submitContextAndTask(final Configuration contextConfiguration,
                            final Configuration taskConfiguration);

  @Override
  void submitContextAndServiceAndTask(final Configuration contextConfiguration,
                                      final Configuration serviceConfiguration,
                                      final Configuration taskConfiguration);
}
