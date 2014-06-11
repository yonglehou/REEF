package com.microsoft.reef.examples.nggroup.bgd.loss;

import javax.inject.Inject;

public final class LogisticLossFunction implements LossFunction {

  /**
   * Trivial constructor.
   */
  @Inject
  public LogisticLossFunction() {
  }

  @Override
  public double computeLoss(final double y, final double f) {
    final double predictedTimesLabel = y * f;
    return Math.log(1 + Math.exp(-predictedTimesLabel));
  }

  @Override
  public double computeGradient(final double y, final double f) {
    final double predictedTimesLabel = y * f;
    return -y/(1 + Math.exp(predictedTimesLabel));
  }

  @Override
  public String toString() {
    return "LogisticLossFunction{}";
  }
}


