package com.microsoft.reef.examples.nggroup.bgd.loss;

import javax.inject.Inject;

public final class WeightedLogisticLossFunction implements LossFunction {

  double pos = 0.0025;
  double neg = 0.9975;
  double posweight;
  double negweight;

  /**
   * Trivial constructor.
   */
  @Inject
  public WeightedLogisticLossFunction() {
    posweight = (this.pos + this.neg) / (2 * this.pos);
    negweight = (this.pos + this.neg) / (2 * this.neg);

  }

  @Override
  public double computeLoss(double y, double f) {
    double predictedTimesLabel = y * f;
    double weight = posweight;
    if (y == -1) {
      weight = negweight;
    }
    if (predictedTimesLabel >= 0) {
      return weight * Math.log(1 + Math.exp(-predictedTimesLabel));
    } else {
      return weight * (-predictedTimesLabel + Math.log(1 + Math.exp(predictedTimesLabel)));
    }
  }

  @Override
  public double computeGradient(double y, double f) {
    double predictedTimesLabel = y * f;
    double weight = posweight;
    if (y == -1) {
      weight = negweight;
    }

    double probability = 0;
    if (predictedTimesLabel >= 0) {
      probability = 1 / (1 + Math.exp(-predictedTimesLabel));
    } else {
      double ExpVal = Math.exp(predictedTimesLabel);
      probability = ExpVal / (1 + ExpVal);
    }

    return (probability - 1) * y * weight;
  }

  @Override
  public String toString() {
    return "WeightedLogisticLossFunction{}";
  }
}


