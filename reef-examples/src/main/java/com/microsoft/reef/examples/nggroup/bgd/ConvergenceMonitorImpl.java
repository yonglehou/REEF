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
package com.microsoft.reef.examples.nggroup.bgd;

import com.microsoft.reef.examples.nggroup.bgd.parameters.Eps;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Iterations;
import com.microsoft.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Simple epsilon-based. implementation of a ConvergenceMonitor.
 */
public final class ConvergenceMonitorImpl implements ConvergenceMonitor {

  private double lastLoss = Double.MAX_VALUE;
  private double delta = Double.MAX_VALUE;
  private int iteration = 0;
  private final double eps;
  private final int maxIter;

  @Inject
  public ConvergenceMonitorImpl(final @Parameter(Eps.class) Double eps,
                                final @Parameter(Iterations.class) Integer maxIter) {
    this.eps = eps;
    this.maxIter = maxIter;
  }

  @Override
  public boolean isNotConverged() {
    return this.iteration <= this.maxIter && this.delta >= this.eps;
  }

  @Override
  public void updateLoss(final double newLoss) {
    if (newLoss == 0.0) { // Make sure we do not miss out on a 0 loss.
      this.delta = 0.0;
    } else {
      this.delta = Math.abs(this.lastLoss - newLoss);
    }
    if (iteration % 5 == 0) {
      this.lastLoss = newLoss;
    }
    this.iteration += 1;
  }
}
