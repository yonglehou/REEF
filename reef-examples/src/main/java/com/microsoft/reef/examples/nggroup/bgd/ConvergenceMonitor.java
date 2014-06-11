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
package com.microsoft.reef.examples.nggroup.bgd;

import com.microsoft.tang.annotations.DefaultImplementation;

/**
 * Base interface for convergence monitors.
 */
@DefaultImplementation(value=ConvergenceMonitorImpl.class)
public interface ConvergenceMonitor {

  /**
   * Returns true if the algorithm is not converged.
   *
   * @return true if the algorithm is not converged.
   */
  public boolean isNotConverged();

  /**
   * Updates the monitor with a newly observed loss value.
   *
   * @param newLoss
   */
  public void updateLoss(final double newLoss);
}
