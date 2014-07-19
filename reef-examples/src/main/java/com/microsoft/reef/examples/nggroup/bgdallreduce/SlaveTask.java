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
package com.microsoft.reef.examples.nggroup.bgdallreduce;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.microsoft.reef.examples.nggroup.bgd.data.Example;
import com.microsoft.reef.examples.nggroup.bgd.data.parser.Parser;
import com.microsoft.reef.examples.nggroup.bgd.loss.LossFunction;
import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.EnableRampup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Iterations;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Lambda;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ModelDimensions;
import com.microsoft.reef.examples.nggroup.bgd.utils.StepSizes;
import com.microsoft.reef.examples.nggroup.bgd.utils.Timer;
import com.microsoft.reef.examples.nggroup.bgdallreduce.operatornames.LineSearchEvaluationsAllReducer;
import com.microsoft.reef.examples.nggroup.bgdallreduce.operatornames.LossAndGradientAllReducer;
import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.Tuple;
import com.microsoft.reef.io.data.loading.api.DataSet;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.GroupCommClient;
import com.microsoft.reef.io.network.nggroup.impl.AllReducer;
import com.microsoft.reef.io.network.util.Utils.Pair;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.reef.io.serialization.SerializableCodec;
import com.microsoft.reef.task.Task;
import com.microsoft.tang.annotations.Parameter;

/**
 *
 */
public class SlaveTask implements Task {

  private static final Logger LOG = Logger.getLogger(SlaveTask.class.getName());

  private static final double FAILURE_PROB = 0.001;
  private final CommunicationGroupClient communicationGroupClient;
  private final AllReducer<Pair<Pair<Double, Integer>, Vector>> lossAndGradientAllReducer;
  private final AllReducer<Pair<Vector, Integer>> lineSearchEvaluationsAllReducer;

  private final List<Example> examples = new ArrayList<>();
  private final DataSet<LongWritable, Text> dataSet;
  private final Parser<String> parser;
  private final LossFunction lossFunction;
  private final StepSizes ts;

  // private Vector model = null;
  private Vector descentDirection = null;

  // boolean sendModel = true;
  private final int dimensions;
  private double minEta = 0;
  private final double lambda;
  private final int maxIters;
  private final ArrayList<Double> losses = new ArrayList<>();
  private final Codec<ArrayList<Double>> lossCodec =
    new SerializableCodec<ArrayList<Double>>();
  private final boolean ignoreAndContinue;

  // Iteration control
  private int iteration = 0;

  @Inject
  public SlaveTask(final GroupCommClient groupCommClient,
    @Parameter(ModelDimensions.class) final int dimensions,
    @Parameter(Lambda.class) final double lambda,
    @Parameter(Iterations.class) final int maxIters,
    @Parameter(EnableRampup.class) final boolean rampup,
    final DataSet<LongWritable, Text> dataSet, final Parser<String> parser,
    final LossFunction lossFunction, final StepSizes ts) {
    this.dataSet = dataSet;
    this.parser = parser;
    this.lossFunction = lossFunction;
    this.ts = ts;
    communicationGroupClient =
      groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    lossAndGradientAllReducer =
      (AllReducer<Pair<Pair<Double, Integer>, Vector>>) communicationGroupClient
        .getAllReducer(LossAndGradientAllReducer.class);
    lineSearchEvaluationsAllReducer =
      (AllReducer<Pair<Vector, Integer>>) communicationGroupClient
        .getAllReducer(LineSearchEvaluationsAllReducer.class);
    // Members from master task
    this.lambda = lambda;
    this.maxIters = maxIters;
    this.ignoreAndContinue = rampup;
    this.dimensions = dimensions;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    Vector model = new DenseVector(dimensions);
    Vector descentDirection = null;
    int curIter = 0;
    int curOp = 1;
    int startOp = 1; // Op for the start of computation, be 1 or 2.
    boolean syncModel = false;
    boolean ownModel = true;
    while (true) {
      // Control message allreduce
      // Get the current iteration, operation
      // and if the input data is required to send.
      if (curOp == 0) {
        Pair<Integer, Pair<Integer, Boolean>> recvState =
          allreducer.apply(new Pair<Integer, Pair<Integer, Boolean>>(curIter,
            new Pair<>(startOp, syncModel)));
        if (recvState != null) {
          syncModel = recvState.second.second.booleanValue();
          ownModel = true;
          if (syncModel) {
            if (curIter != recvState.first.intValue()
              && curOp != recvState.second.first.intValue()) {
              ownModel = false;
            }
          }
          curIter = recvState.first.intValue();
          // Update curOp to 1 or 2
          curOp = recvState.second.first.intValue();
        } else {
          // curOp won't change
          // Check and update at the bottom
        }
      }
      if (curOp == 1) {
        Vector recvModel = null;
        if (syncModel) {
          if (!ownModel) {
            model = new DenseVector(new double[0]);
          }
          recvModel = allreducer.apply(model);
          if (recvModel != null) {
            model = recvModel;
            syncModel = false;
            ownModel = true;
          } else {
            // curIter doesn't change
            // curOp won't increase
            // syncModel keeps true
            // Check and update at the bottom
            // Resync at the iteration start
            startOp = 1;
          }
        }
        if (ownModel) {
          Pair<Pair<Double, Integer>, Vector> lossAndGradient =
            allreduceLossAndGradient(model);
          if (lossAndGradient != null) {
            System.out.println("Loss: " + lossAndGradient.first.first
              + " #ex: " + lossAndGradient.first.second);
            System.out.println("Gradient: " + lossAndGradient.second + " #ex: "
              + lossAndGradient.first.second);
            Vector gradient = regularizeLossAndGradient(model, lossAndGradient);
            if (converged(gradient.norm2())) {
              break;
            }
            descentDirection = getDescentDirection(gradient);
            curOp++; // Continue to next op.
          } else {
            startOp = 1;
          }
        }
      }
      if (curOp == 2) {
        if (syncModel) {
          Pair<Vector, Vector> modelPair = new Pair<>(model, descentDirection);
          if (!ownModel) {
            modelPair =
              new Pair<Vector, Vector>(new DenseVector(new double[0]),
                new DenseVector(new double[0]));
          }
          Pair<Vector, Vector> recvModelPair = allreducer.apply(modelPair);
          if (recvModelPair != null) {
            model = recvModelPair.first;
            descentDirection = recvModelPair.second;
            syncModel = false;
            ownModel = true;
          } else {
            // curIter doesn't change
            // curOp won't increase
            // Check and update at the bottom
            // Resync at the iteration start
            startOp = 2;
          }
        }
        if (ownModel) {
          // Line search
          Pair<Vector, Integer> lineSearchEvals =
            allreduceLineSearch(syncModel, model, descentDirection);
          if (lineSearchEvals != null) {
            updateModel(model, descentDirection, lineSearchEvals);
          } else {
            startOp = 2;
          }
        }
      }
      boolean isOK = checkAndUpdate();
      if (!isOK) {
        curOp = 0;
      } else {
        // If the iteration is successful,
        // go back to op 1 and evaluate the model.
        curIter++;
        curOp = 1;
      }
    }
    for (final Double loss : losses) {
      System.out.println(loss);
    }
    return lossCodec.encode(losses);
  }

  private boolean checkAndUpdate() {
    communicationGroupClient.checkIteration();
    if (communicationGroupClient.isCurrentIterationFailed()) {
      communicationGroupClient.updateTopologyAndIteration();
      return false;
    } else if (communicationGroupClient.isNewTaskAvailable()) {
      // May or may not update
      communicationGroupClient.updateTopologyAndIteration();
      return false;
    }
    communicationGroupClient.updateIteration();
    return true;
  }

  private boolean converged(final double gradNorm) {
    return iteration >= maxIters || Math.abs(gradNorm) <= 1e-3;
  }

  private Pair<Pair<Double, Integer>, Vector> allreduceLossAndGradient(
    Vector model) throws NetworkException, InterruptedException {
    Pair<Pair<Double, Integer>, Vector> lossAndGradient =
      lossAndGradientAllReducer.apply(computeLossAndGradient(model));
    return lossAndGradient;
  }

  private Pair<Pair<Double, Integer>, Vector> computeLossAndGradient(
    Vector model) {
    if (examples.isEmpty()) {
      loadData();
    }
    final Vector gradient = new DenseVector(model.size());
    double loss = 0.0;
    for (final Example example : examples) {
      final double f = example.predict(model);
      final double g = lossFunction.computeGradient(example.getLabel(), f);
      example.addGradient(gradient, g);
      loss += lossFunction.computeLoss(example.getLabel(), f);
    }
    return new Pair<>(new Pair<>(loss, examples.size()), gradient);
  }

  private Vector regularizeLossAndGradient(Vector model,
    final Pair<Pair<Double, Integer>, Vector> lossAndGradient) {
    Vector gradient = null;
    // Why timer?
    try (Timer t = new Timer("Regularize(Loss) + Regularize(Gradient)")) {
      final double loss =
        regularizeLoss(lossAndGradient.first.first,
          lossAndGradient.first.second, model);
      System.out.println("RegLoss: " + loss);
      gradient =
        regularizeGrad(lossAndGradient.second, lossAndGradient.first.second,
          model);
      System.out.println("RegGradient: " + gradient);
      losses.add(loss);
    }
    return gradient;
  }

  private double regularizeLoss(final double loss, final int numEx,
    final Vector model) {
    return regularizeLoss(loss, numEx, model.norm2Sqr());
  }

  private double regularizeLoss(final double loss, final int numEx,
    final double modelNormSqr) {
    return loss / numEx + ((lambda / 2) * modelNormSqr);
  }

  private Vector regularizeGrad(final Vector gradient, final int numEx,
    final Vector model) {
    gradient.scale(1.0 / numEx);
    gradient.multAdd(lambda, model);
    return gradient;
  }

  private Vector getDescentDirection(final Vector gradient) {
    gradient.scale(-1);
    System.out.println("DescentDirection: " + gradient);
    return gradient;
  }

  private Pair<Vector, Integer> allreduceLineSearch(boolean sendModel,
    Vector model, Vector descentDirection) throws NetworkException,
    InterruptedException {
    Pair<Vector, Integer> lineSearchEvals =
      lineSearchEvaluationsAllReducer
        .apply(lineSearch(model, descentDirection));
    return lineSearchEvals;
  }

  /**
   * @param modelAndDescentDir
   * @return
   */
  private Pair<Vector, Integer> lineSearch(Vector model, Vector descentDirection) {
    if (examples.isEmpty()) {
      loadData();
    }
    final Vector zed = new DenseVector(examples.size());
    final Vector ee = new DenseVector(examples.size());
    for (int i = 0; i < examples.size(); i++) {
      final Example example = examples.get(i);
      double f = example.predict(model);
      zed.set(i, f);
      f = example.predict(descentDirection);
      ee.set(i, f);
    }
    final double[] t = ts.getT();
    final Vector evaluations = new DenseVector(t.length);
    int i = 0;
    for (final double d : t) {
      double loss = 0;
      for (int j = 0; j < examples.size(); j++) {
        final Example example = examples.get(j);
        final double val = zed.get(j) + d * ee.get(j);
        loss += lossFunction.computeLoss(example.getLabel(), val);
      }
      evaluations.set(i++, loss);
    }
    return new Pair<>(evaluations, examples.size());
  }

  private void updateModel(Vector model, Vector descentDirection,
    final Pair<Vector, Integer> lineSearchEvals) {
    try (Timer t = new Timer("GetDescentDirection + FindMinEta + UpdateModel")) {
      minEta = findMinEta(model, descentDirection, lineSearchEvals);
      descentDirection.scale(minEta);
      model.add(descentDirection);
    }
    System.out.println("New Model: " + model);
  }

  private double findMinEta(final Vector model, final Vector descentDir,
    final Pair<Vector, Integer> lineSearchEvals) {
    final double wNormSqr = model.norm2Sqr();
    final double dNormSqr = descentDir.norm2Sqr();
    final double wDotd = model.dot(descentDir);
    final double[] t = ts.getT();
    int i = 0;
    for (final double eta : t) {
      final double modelNormSqr =
        wNormSqr + (eta * eta) * dNormSqr + 2 * eta * wDotd;
      final double loss =
        regularizeLoss(lineSearchEvals.first.get(i), lineSearchEvals.second,
          modelNormSqr);
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
   *
   */
  private void loadData() {
    LOG.info("Loading data");
    int i = 0;
    for (final Pair<LongWritable, Text> examplePair : dataSet) {
      final Example example = parser.parse(examplePair.second.toString());
      examples.add(example);
      if (++i % 2000 == 0) {
        LOG.info("Done parsing " + i + " lines");
      }
    }
  }

  private void failPerhaps() {
    // Temporarily stop generating failure
    // if (Math.random() < FAILURE_PROB) {
    // throw new RuntimeException("Simulated Failure");
    // }
  }
}
