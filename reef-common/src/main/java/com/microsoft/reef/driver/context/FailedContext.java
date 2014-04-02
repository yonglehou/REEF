/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.driver.context;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.common.Failure;
import com.microsoft.reef.util.Optional;

/**
 * Represents an EvaluatorContext that failed.
 * A typical case would be that its StartHandler threw an exception.
 * <p/>
 * The underlying Evaluator is still accessible and a new context can be established.
 */
@Public
@Provided
@DriverSide
public interface FailedContext extends Failure, ContextBase {

  /**
   * @return the new top of the stack of context if there is one.
   */
  public abstract Optional<ActiveContext> getParentContext();
}
