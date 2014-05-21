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
package com.microsoft.reef.driver.context;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.common.Failure;
import com.microsoft.reef.util.Optional;

/**
 * Represents Context that failed.
 * A typical case would be that its ContextStartHandler threw an exception.
 * <p/>
 * The underlying Evaluator is still accessible and a new context can be established. Note that REEF can't guarantee
 * consistency of the Evaluator for all applications. It is up to the application to decide whether it is safe to keep
 * using the Evaluator.
 */
@Public
@Provided
@DriverSide
public interface FailedContext extends Failure, ContextBase {

  /**
   * @return the new top of the stack of context if there is one.
   */
  Optional<ActiveContext> getParentContext();
}
