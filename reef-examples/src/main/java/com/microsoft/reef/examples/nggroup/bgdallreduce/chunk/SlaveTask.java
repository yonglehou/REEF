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
package com.microsoft.reef.examples.nggroup.bgdallreduce.chunk;

import com.microsoft.reef.examples.nggroup.bgd.data.Example;
import com.microsoft.reef.examples.nggroup.bgd.data.parser.Parser;
import com.microsoft.reef.examples.nggroup.bgd.loss.LossFunction;
import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.examples.nggroup.bgd.parameters.*;
import com.microsoft.reef.examples.nggroup.bgd.utils.StepSizes;
import com.microsoft.reef.examples.nggroup.bgd.utils.Timer;
import com.microsoft.reef.examples.nggroup.bgdallreduce.ControlMessage;
import com.microsoft.reef.examples.nggroup.bgdallreduce.operatornames.*;
import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.Tuple;
import com.microsoft.reef.io.data.loading.api.DataSet;
import com.microsoft.reef.io.network.group.operators.AllReduce;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.GroupCommClient;
import com.microsoft.reef.io.network.util.Utils.Pair;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.reef.io.serialization.SerializableCodec;
import com.microsoft.reef.task.Task;
import com.microsoft.tang.annotations.Parameter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import javax.inject.Inject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 *
 */
public class SlaveTask implements Task {

  private static final Logger LOG = Logger.getLogger(SlaveTask.class.getName());

  private static final double FAILURE_PROB = 0.1;
  private final CommunicationGroupClient communicationGroupClient;
  private final AllReduce<ControlMessage> controlMessageAllReducer;
  private final AllReduce<Vector> modelAllReducer;
  private final AllReduce<Pair<Pair<Double, Integer>, Vector>> lossAndGradientAllReducer;
  private final AllReduce<Pair<Vector, Vector>> modelDescentDirectionAllReducer;
  private final AllReduce<Pair<Vector, Integer>> lineSearchEvaluationsAllReducer;

  private final List<Example> examples = new ArrayList<>();
  private final DataSet<LongWritable, Text> dataSet;
  private final Parser<String> parser;
  private final LossFunction lossFunction;
  private final StepSizes ts;

  private final int dimensions;
  private final int numChunks = 5;
  private final int numTotalTasks = 5;
  private final boolean evaluateModel = false;
  private final int recordInterval = 5;
  // Model ID recorder
  private Set<Integer> modelSet = new TreeSet<>();
  
  private double minEta = 0;
  private final double lambda;
  private final int maxIters;
  private final ArrayList<Double> losses = new ArrayList<>();
  private final Codec<ArrayList<Double>> lossCodec =
    new SerializableCodec<ArrayList<Double>>();
  private final boolean ignoreAndContinue;

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
    controlMessageAllReducer =
      (AllReduce<ControlMessage>) communicationGroupClient
        .getAllReducer(ControlMessageAllReducer.class);
    modelAllReducer =
      (AllReduce<Vector>) communicationGroupClient
        .getAllReducer(ModelAllReducer.class);
    lossAndGradientAllReducer =
      (AllReduce<Pair<Pair<Double, Integer>, Vector>>) communicationGroupClient
        .getAllReducer(LossAndGradientAllReducer.class);
    modelDescentDirectionAllReducer =
      (AllReduce<Pair<Vector, Vector>>) communicationGroupClient
        .getAllReducer(ModelDescentDirectionAllReducer.class);
    lineSearchEvaluationsAllReducer =
      (AllReduce<Pair<Vector, Integer>>) communicationGroupClient
        .getAllReducer(LineSearchEvaluationsAllReducer.class);
    // Members from master task
    this.lambda = lambda;
    this.maxIters = maxIters;
    this.ignoreAndContinue = rampup;
    this.dimensions = dimensions;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    String taskID = communicationGroupClient.getTaskID();
    Vector model = new DenseVector(dimensions);
    Vector descentDirection = null;
    int startIte = 1;
    int startOp = 1; // Op for the start of computation, be 1 or 2.
    boolean syncModel = false;
    String leadingTaskID = null;
    int curIte = 0;
    int curOp = 0;
    boolean stop = false;
    int numRunningTasks = 0;
    // Ramp up control
    int fullIteration = -1;

    // Data loading
    if (examples.isEmpty()) {
      loadData();
    }

    // while(true){
    // (1): Synchronize Topology
    // (2): Perform controlMessageAllReducer
    // if no result: continue
    // (3): Perform lossAndGradientAllReducer
    // if no result: continue
    // (4): Update model and break criterion
    // }

    while (true) {
      DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
      Date date = new Date();
      try (Timer timer =
        new Timer(dateFormat.format(date) + " " + taskID + " ITERATION")) {
        checkAndUpdate();
        // Control message allreduce
        // Get the current iteration, operation
        // and if the input data is required to send.
        System.out.println(taskID + " SYNC ITERATION.");
        // MasterTask decides to stop the computation
        if (stop) {
          if (taskID.compareTo("MasterTask") == 0) {
            stop = true;
          } else {
            stop = false;
          }
        }
        failPerhaps(taskID);
        ControlMessage recvState = null;
        try (Timer t = new Timer("Control Message AllReduce")) {
          recvState =
            controlMessageAllReducer.apply(new ControlMessage(startIte,
              startOp, taskID, syncModel, stop, 1, fullIteration));
        }
        if (recvState != null) {
          curIte = recvState.iteration;
          curOp = recvState.operation;
          leadingTaskID = recvState.taskID;
          syncModel = recvState.syncData;
          stop = recvState.stop;
          numRunningTasks = recvState.numRunningTasks;
          
          if (fullIteration == -1) {
            fullIteration = recvState.fullIteration;
          }
          
          System.out.println(taskID + " ITERATION " + curIte + " OP " + curOp
            + " SYNC MODEL " + syncModel + " LEADING TASK ID: " + leadingTaskID
            + " STOP: " + stop + " NUM RUNNING TASKS: " + numRunningTasks
            + " FULL ITERATION " + fullIteration);
        } else {
          System.out.println(taskID + " ITERATION SYNC FAILS");
          curIte = 0;
          curOp = 0;
          numRunningTasks = 0;
        }
        if (stop) {
          break;
        }
        if (curOp == 1) {
          Vector recvModel = null;
          if (syncModel) {
            List<Vector> recvModelList = null;
            if (taskID.compareTo(leadingTaskID) != 0) {
              List<Vector> emptyModelList = new LinkedList<>();
              emptyModelList.add(new DenseVector(new double[0]));
              try (Timer t = new Timer("Model AllReduce (Empty)")) {
                failPerhaps(taskID);
                recvModelList = modelAllReducer.apply(emptyModelList);
              }
              if (recvModelList != null) {
                recvModel = copyVectorFromList(recvModelList);
              } else {
                recvModel = null;
              }
            } else {
              List<Vector> modelList = copyVectorToList(model, numChunks);
              try (Timer t = new Timer("Model AllReduce (Full)")) {
                failPerhaps(taskID);
                recvModelList = modelAllReducer.apply(modelList);
              }
              if (recvModelList != null) {
                // Here model is recvModel
                recvModel = model;
              } else {
                recvModel = null;
              }
            }
          }
          if (!syncModel || recvModel != null) {
            if (recvModel != null) {
              model = recvModel;
            }
            startIte = curIte;
            startOp = 1;
            syncModel = false;
            failPerhaps(taskID);
            Pair<Pair<Double, Integer>, Vector> lossAndGradient =
              allreduceLossAndGradient(model);
            if (lossAndGradient != null) {
              System.out.println("LOSS: " + lossAndGradient.first.first
                + " #EX: " + lossAndGradient.first.second);
              System.out.println("GRADIENT: " + lossAndGradient.second
                + " #EX: " + lossAndGradient.first.second);
              Vector gradient =
                regularizeLossAndGradient(model, lossAndGradient);
              if (converged(startIte, gradient.norm2(), fullIteration)) {
                stop = true;
              } else {
                descentDirection = getDescentDirection(gradient);
                startOp = 2;
                // Continue to next op.
                curOp = 2;
              }
            }
          }
        }
        if (!stop && curOp == 2) {
          Pair<Vector, Vector> recvModelPair = null;
          if (syncModel) {
            List<Pair<Vector, Vector>> recvModelPairList = null;
            if (taskID.compareTo(leadingTaskID) != 0) {
              List<Pair<Vector, Vector>> emptyModePairlList =
                new LinkedList<>();
              emptyModePairlList.add(new Pair<Vector, Vector>(new DenseVector(
                new double[0]), new DenseVector(new double[0])));
              try (Timer t =
                new Timer("Model And Descent Direction AllReduce (Empty)")) {
                failPerhaps(taskID);
                recvModelPairList =
                  modelDescentDirectionAllReducer.apply(emptyModePairlList);
              }
              if (recvModelPairList != null) {
                recvModelPair = copyModelPairFromList(recvModelPairList);
              } else {
                recvModelPair = null;
              }
            } else {
              List<Vector> modelList = copyVectorToList(model, numChunks);
              List<Vector> descentDirectionList =
                copyVectorToList(descentDirection, numChunks);
              List<Pair<Vector, Vector>> modelPairList =
                createModelPairList(modelList, descentDirectionList);
              try (Timer t =
                new Timer("Model And Descent Direction AllReduce (Full)")) {
                failPerhaps(taskID);
                recvModelPairList =
                  modelDescentDirectionAllReducer.apply(modelPairList);
              }
              if (recvModelPairList != null) {
                recvModelPair = new Pair<>(model, descentDirection);
              } else {
                recvModelPair = null;
              }
            }
          }
          if (!syncModel || recvModelPair != null) {
            if (recvModelPair != null) {
              model = recvModelPair.first;
              descentDirection = recvModelPair.second;
            }
            startIte = curIte;
            startOp = 2;
            syncModel = false;
            // Line search
            failPerhaps(taskID);
            // AllReduce with fixed-sized data, no need to chunk
            Pair<Vector, Integer> lineSearchEvals =
              allreduceLineSearch(model, descentDirection);
            if (lineSearchEvals != null) {
              updateModel(model, descentDirection, lineSearchEvals);
              
              // Write model if model evaluation is enabled
              try (Timer t = new Timer(taskID + " Write Model")) {
                writeModel(taskID, startIte, model);
              }
              
              // Check if the current iteration is the full iteration
              // Record the first time all tasks are present in one iteration
              if (numRunningTasks == numTotalTasks && fullIteration == -1) {
                fullIteration = startIte;
              }
              
              startIte++;
              startOp = 1;
            }
          }
        }
        // checkAndUpdate();
      }
    }
    for (final Double loss : losses) {
      System.out.println(taskID + " LOSS " + loss);
    }
    
    // Note: this part is not FAULT TOLERANT
    // Model Evaluation
    if (evaluateModel) {
      TreeMap<Integer, Double> lossMap = new TreeMap<>();
      for (int i = 1; i < fullIteration + maxIters; i += recordInterval) {
        if (taskID.compareTo("MasterTask") != 0) {
          model = modelAllReducer.apply(new DenseVector(new double[0]));
        } else {
          // Read model
          model = readModel(taskID, i);
          model = modelAllReducer.apply(model);
        }
        if (model.size() != 0) {
          Pair<Pair<Double, Integer>, Vector> lossAndGradient =
            allreduceLossAndGradient(model);
          final double loss =
            regularizeLoss(lossAndGradient.first.first,
              lossAndGradient.first.second, model);

          lossMap.put(i, loss);
        }
      }
      for (Entry<Integer, Double> entry : lossMap.entrySet()) {
        System.out.println(taskID + " MODEL " + entry.getKey()
          + " EVALUATION LOSS: " + entry.getValue());
      }
    }
    return lossCodec.encode(losses);
  }

  private void writeModel(String taskID, int iteration, Vector model) {
    if (taskID.compareTo("MasterTask") == 0 && evaluateModel
      && iteration % recordInterval == 1) {
      modelSet.add(iteration);
      try {
        DataOutputStream dout =
          new DataOutputStream(new FileOutputStream(new File("Model_"
            + iteration)));
        dout.writeInt(model.size());
        double[] doubles = ((DenseVector) model).getValues();
        for (int i = 0; i < doubles.length; i++) {
          dout.writeDouble(doubles[i]);
        }
        dout.flush();
        dout.close();
      } catch (Exception e) {
        e.printStackTrace(System.out);
      }
    }
  }

  private Vector readModel(String taskID, int iteration) {
    if (taskID.compareTo("MasterTask") == 0) {
      try {
        DataInputStream din =
          new DataInputStream(new FileInputStream(
            new File("Model_" + iteration)));
        int modelSize = din.readInt();
        double[] doubles = new double[modelSize];
        for (int i = 0; i < modelSize; i++) {
          doubles[i] = din.readDouble();
        }
        din.close();
        return new DenseVector(doubles);
      } catch (Exception e) {
        e.printStackTrace(System.out);
      }
    }
    return new DenseVector(new double[0]);
  }

  private Vector copyVectorFromList(List<Vector> vectorList) {
    int count = 0;
    for (int i = 0; i < vectorList.size(); i++) {
      count += vectorList.get(i).size();
    }
    double[] doubles = new double[count];
    int pos = 0;
    for (int i = 0; i < vectorList.size(); i++) {
      double[] values = ((DenseVector) vectorList.get(i)).getValues();
      System.arraycopy(values, 0, doubles, pos, values.length);
      pos += values.length;
    }
    return new DenseVector(doubles);
  }

  private List<Vector> copyVectorToList(Vector vector, int numChunks) {
    List<Vector> vectorList = new LinkedList<>();
    double[] values = ((DenseVector) vector).getValues();
    int pos = 0;
    int size = vector.size() / numChunks;
    int rest = vector.size() % numChunks;
    if(size == 0) {
      size = vector.size();
      rest = 0;
    }
    System.out.println("Chunk size: " + size + ", rest size: " + rest);
    int sizePerVector = size;
    for (int i = 0; i < numChunks; i++) {
      if (rest > 0) {
        sizePerVector = size + 1;
        rest--;
      } else {
        sizePerVector = size;
      }
      double[] doubles = new double[sizePerVector];
      System.arraycopy(values, pos, doubles, 0, sizePerVector);
      pos += sizePerVector;
      vectorList.add(new DenseVector(doubles));
    }
    return vectorList;
  }

  private Pair<Vector, Vector> copyModelPairFromList(
    List<Pair<Vector, Vector>> modelPairList) {
    List<Vector> modelList = new LinkedList<>();
    List<Vector> descentDirectionList = new LinkedList<>();
    for (int i = 0; i < modelPairList.size(); i++) {
      modelList.add(modelPairList.get(i).first);
      descentDirectionList.add(modelPairList.get(i).second);
    }
    Vector model = copyVectorFromList(modelList);
    Vector descentDirection = copyVectorFromList(descentDirectionList);
    return new Pair<Vector, Vector>(model, descentDirection);
  }

  private List<Pair<Vector, Vector>> createModelPairList(
    List<Vector> modelList, List<Vector> descentDirectionList) throws Exception {
    List<Pair<Vector, Vector>> modelPairList = new LinkedList<>();
    int modelListSize= modelList.size();
    int descentDirectionListSize = descentDirectionList.size();
    if (modelListSize != descentDirectionListSize) {
      System.out.println("Model and Descent Direction data are not matched.");
      throw new Exception("Model and Descent Direction data are not matched.");
    }
    for (int i = 0; i < modelList.size(); i++) {
      modelPairList.add(new Pair<Vector, Vector>(modelList.get(i),
        descentDirectionList.get(i)));
    }
    return modelPairList;
  }

  private boolean checkAndUpdate() {
    boolean isFailed = false;
    communicationGroupClient.checkIteration();
    if (communicationGroupClient.isCurrentIterationFailed()) {
      isFailed = true;
    }
    communicationGroupClient.updateIteration();
    return isFailed;
  }

  private boolean converged(final int iteration, final double gradNorm,
    final int fullIteration) {
    if (fullIteration != -1 && (iteration - fullIteration + 1) >= maxIters) {
      return true;
    }
    return false;
    // return iteration >= maxIters || Math.abs(gradNorm) <= 1e-3;
  }

  private Pair<Pair<Double, Integer>, Vector> allreduceLossAndGradient(
    final Vector model) throws NetworkException, InterruptedException {
    Pair<Pair<Double, Integer>, Vector> localLossAndGradient =
      computeLossAndGradient(model);
    List<Pair<Pair<Double, Integer>, Vector>> localLossAndGradientList =
      copyLocalLossAndGradientToList(localLossAndGradient);
    List<Pair<Pair<Double, Integer>, Vector>> lossAndGradientList = null;
    try (Timer t = new Timer("Loss And Gradient AllReduce")) {
      lossAndGradientList =
        lossAndGradientAllReducer.apply(localLossAndGradientList);
    }
    if (lossAndGradientList == null) {
      return null;
    } else {
      return copyLossAndGradientFromList(lossAndGradientList);
    }
  }

  private List<Pair<Pair<Double, Integer>, Vector>> copyLocalLossAndGradientToList(
    Pair<Pair<Double, Integer>, Vector> localLossAndGradient) {
    List<Pair<Pair<Double, Integer>, Vector>> localLossAndGradientList =
      new LinkedList<>();
    Vector gradient = localLossAndGradient.second;
    List<Vector> gradientList = copyVectorToList(gradient, numChunks);
    for (int i = 0; i < gradientList.size(); i++) {
      localLossAndGradientList.add(new Pair<>(new Pair<>(new Double(
        localLossAndGradient.first.first.doubleValue()), new Integer(
        localLossAndGradient.first.second.intValue())), gradientList.get(i)));
    }
    return localLossAndGradientList;
  }
  
  private Pair<Pair<Double, Integer>, Vector> copyLossAndGradientFromList(
    List<Pair<Pair<Double, Integer>, Vector>> lossAndGradientList) {
    List<Vector> gradientList = new LinkedList<>();
    for (int i = 0; i < lossAndGradientList.size(); i++) {
      gradientList.add(lossAndGradientList.get(i).second);
    }
    Vector gradient = copyVectorFromList(gradientList);
    Pair<Pair<Double, Integer>, Vector> lossAndGradient =
      new Pair<>(new Pair<>(
        lossAndGradientList.get(0).first.first.doubleValue(),
        lossAndGradientList.get(0).first.second.intValue()), gradient);
    return lossAndGradient;
  }



  private Pair<Pair<Double, Integer>, Vector> computeLossAndGradient(
    Vector model) {
    // Model is not changed in this function
    if (examples.isEmpty()) {
      loadData();
    }
    final Vector gradientVector = new DenseVector(model.size());
    double loss = 0.0;
    for (final Example example : examples) {
      final double f = example.predict(model);
      final double g = lossFunction.computeGradient(example.getLabel(), f);
      example.addGradient(gradientVector, g);
      loss += lossFunction.computeLoss(example.getLabel(), f);
    }
    return new Pair<>(new Pair<>(loss, examples.size()), gradientVector);
  }

  private Vector regularizeLossAndGradient(final Vector model,
    final Pair<Pair<Double, Integer>, Vector> lossAndGradient) {
    // Model is not changed in this function
    Vector gradient = null;
    // try (Timer t = new Timer("Regularize(Loss) + Regularize(Gradient)")) {
    final double loss =
      regularizeLoss(lossAndGradient.first.first, lossAndGradient.first.second,
        model);
    System.out.println("REGULIZED LOSS: " + loss);
    gradient =
      regularizeGrad(lossAndGradient.second, lossAndGradient.first.second,
        model);
    System.out.println("REGULIZED GRADIENT: " + gradient);
    losses.add(loss);
    // }
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
    System.out.println("DESCENT DIRECTION: " + gradient);
    return gradient;
  }

  private Pair<Vector, Integer> allreduceLineSearch(Vector model,
    Vector descentDirection) throws NetworkException, InterruptedException {
    Pair<Vector, Integer> localLineSearchEvals =
      lineSearch(model, descentDirection);
    Pair<Vector, Integer> lineSearchEvals = null;
    try (Timer t = new Timer("LineSearch AllReduce")) {
      lineSearchEvals =
        lineSearchEvaluationsAllReducer.apply(localLineSearchEvals);
    }
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
    // try (Timer t = new
    // Timer("GetDescentDirection + FindMinEta + UpdateModel")) {
    minEta = findMinEta(model, descentDirection, lineSearchEvals);
    descentDirection.scale(minEta);
    model.add(descentDirection);
    // }
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

  private void failPerhaps(String taskID) {
    if (Math.random() < FAILURE_PROB && taskID.compareTo("MasterTask") != 0) {
      System.out.println("Simulated Failure");
      throw new RuntimeException("Simulated Failure");
    }
  }
}
