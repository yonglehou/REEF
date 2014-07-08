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

import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.io.Tuple;
import com.microsoft.reef.io.network.util.Utils.Pair;

/**
 *
 */
public class TestVector {

  public static final StepSizes ts = new StepSizes();
  public static final double lambda = 1e-5;

  /**
   * @param first
   * @param model
   * @return
   */
  public static double regularizeLoss(final double loss, final int numEx, final Vector model) {
    return loss / numEx + ((lambda / 2) * model.norm2Sqr());
  }

  public static double regularizeLoss(final double loss, final int numEx, final double modelNormSqr) {
    return loss / numEx + ((lambda / 2) * modelNormSqr);
  }

  public static double findMinEtaEff(final Vector model, final Vector descentDir, final Pair<Vector, Integer> lineSearchEvals) {
    final double wNormSqr = model.norm2Sqr();
    final double dNormSqr = descentDir.norm2Sqr();
    final double wDotd = model.dot(descentDir);
    final double[] t = ts.getT();
    int i = 0;
    for (final double eta : t) {
      final double modelNormSqr = wNormSqr + (eta * eta) * dNormSqr + 2 * eta * wDotd;
      final double loss = regularizeLoss(lineSearchEvals.first.get(i), lineSearchEvals.second, modelNormSqr);
      lineSearchEvals.first.set(i, loss);
      ++i;
    }
    System.out.println("Regularized LineSearchEvals: " + lineSearchEvals.first);
    final Tuple<Integer, Double> minTup = lineSearchEvals.first.min();
    System.out.println("MinTup: " + minTup);
    final double minT = t[minTup.getKey()];
    System.out.println("MinT: " + minT);
    return minT;
  }

  public static double findMinEta(final Vector model, final Vector descentDir, final Pair<Vector, Integer> lineSearchEvals) {
    final double[] t = ts.getT();
    int i = 0;
    for (final double d : t) {
      final Vector newModel = DenseVector.copy(model);
      newModel.multAdd(d, descentDir);
      final double loss = regularizeLoss(lineSearchEvals.first.get(i), lineSearchEvals.second, newModel);
      lineSearchEvals.first.set(i, loss);
      ++i;
    }
    System.out.println("Regularized LineSearchEvals: " + lineSearchEvals.first);
    final Tuple<Integer, Double> minTup = lineSearchEvals.first.min();
    System.out.println("MinTup: " + minTup);
    final double minT = t[minTup.getKey()];
    System.out.println("MinT: " + minT);
    return minT;
  }

  /**
   * @param args
   */
  public static void main(final String[] args) {
    final double[] modelValues = new double[3000000];
    final double[] ddValues = new double[3000000];
    for (int i = 0; i < modelValues.length; i++) {
      modelValues[i] = Math.random();
      ddValues[i] = Math.random();
    }

    final double[] lineSearchValues = new double[ts.getGridSize()];
    for (int i = 0; i < lineSearchValues.length; i++) {
      lineSearchValues[i] = Math.random();
    }
    final Vector model = new DenseVector(modelValues);
    final Vector dd = new DenseVector(ddValues);
    final Vector lineSearchEvals = new DenseVector(lineSearchValues);

    try (Timer timer = new Timer("FindMinEta")) {
      findMinEtaEff(model, dd, new Pair<>(lineSearchEvals, 1000000));
    }
    try (Timer timer = new Timer("FindMinEta")) {
      findMinEta(model, dd, new Pair<>(lineSearchEvals, 1000000));
    }
  }

  public static class Timer implements AutoCloseable {

    private final String description;
    private final long t1;

    public Timer(final String description) {
      this.description = description;
      t1 = System.currentTimeMillis();
    }

    @Override
    public void close() {
      final long t2 = System.currentTimeMillis();
      System.out.println(description + " took " + (t2 - t1) / 1000.0 + " sec");
    }

  }

}
