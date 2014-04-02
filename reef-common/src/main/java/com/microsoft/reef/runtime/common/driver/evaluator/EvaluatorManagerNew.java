package com.microsoft.reef.runtime.common.driver.evaluator;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.CompletedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.driver.evaluator.EvaluatorType;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.io.naming.Identifiable;
import com.microsoft.reef.proto.EvaluatorRuntimeProtocol;
import com.microsoft.reef.runtime.common.protocol.ErrorMessage;
import com.microsoft.reef.runtime.common.protocol.EvaluatorHeartbeat;
import com.microsoft.reef.runtime.common.protocol.EvaluatorState;
import com.microsoft.reef.runtime.common.protocol.EvaluatorStateTransition;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A replacement for EvaluatorManager.
 */
// TODO: Rename to EvaluatorManager and update the comment above
@DriverSide
@Private
public final class EvaluatorManagerNew implements AutoCloseable, Identifiable {
  private static final Logger LOG = Logger.getLogger(EvaluatorManagerNew.class.getName());
  private final ContextManager contextManager;
  private final EvaluatorDescriptor evaluatorDescriptor;
  private final String evaluatorId;
  private final EvaluatorMessageDispatcher messageDispatcher;
  private int sequenceNumber = 0;
  private EvaluatorState evaluatorState = EvaluatorState.INIT;
  private final TaskManager taskManager;
  private final EvaluatorControlHandler evaluatorControlHandler;

  public EvaluatorManagerNew(final ContextManager contextManager,
                             final @Parameter(EvaluatorManager.EvaluatorDescriptorName.class) EvaluatorDescriptorImpl evaluatorDescriptor,
                             final @Parameter(EvaluatorManager.EvaluatorIdentifier.class) String evaluatorId,
                             final EvaluatorMessageDispatcher messageDispatcher,
                             final TaskManager taskManager, EvaluatorControlHandler evaluatorControlHandler) {
    this.contextManager = contextManager;
    this.evaluatorDescriptor = evaluatorDescriptor;
    this.evaluatorId = evaluatorId;
    this.messageDispatcher = messageDispatcher;
    this.taskManager = taskManager;
    this.evaluatorControlHandler = evaluatorControlHandler;
  }


  /**
   * Process a hearbeat sent by an Evaluator.
   *
   * @param heartbeat
   */
  public synchronized void handleEvaluatorHeartbeat(final EvaluatorHeartbeat heartbeat) {
    for (final EvaluatorStateTransition transition : heartbeat.getStateTransitions()) {
      assert (transition.isLegal());
    }

    { // Handle sequence number
      assert (heartbeat.getSequenceNumber() > this.sequenceNumber);
      this.sequenceNumber = heartbeat.getSequenceNumber();
    }
    if (this.evaluatorState != heartbeat.getState()) {
      // We need to do something.
      switch (heartbeat.getState()) {
        case RUNNING:
          // We assume there also to be a context in it. contextManager will handle the notification to the user.
          break;
        case DONE:
          onEvaluatorCompleted(heartbeat);
          break;
        case FAILED:
          this.onEvaluatorFailure(heartbeat);
          break;
        case KILLED:
          this.onEvaluatorCompleted(heartbeat);
          break;
        default:
          throw new RuntimeException("Unknown evaluator state: '" + heartbeat.getState() + "'");
      }
      this.evaluatorState = heartbeat.getState();
    }


    // Process the context heartbeats
    this.contextManager.handleContextHeartbeat(heartbeat.getContextHeartbeat());

    // Process the task heartbeats
    if (heartbeat.getTaskHeartbeat().isPresent()) {
      this.taskManager.handleTaskHeartbeat(heartbeat.getTaskHeartbeat().get());
    }

    LOG.log(Level.FINEST, "DONE with evaluator heartbeat");
  }

  /**
   * Assembles an CompletedEvaluator.
   *
   * @param heartbeat
   * @return an CompletedEvaluator.
   */
  private synchronized void onEvaluatorCompleted(final EvaluatorHeartbeat heartbeat) {
    assert (EvaluatorState.DONE == heartbeat.getState() || EvaluatorState.KILLED == heartbeat.getState());
    final CompletedEvaluator completedEvaluator = new CompletedEvaluatorImpl(this.getId());
    this.messageDispatcher.onEvaluatorCompleted(completedEvaluator);
    this.close();
  }

  /**
   * Assembles a FailedEvaluator.
   *
   * @param heartbeat
   * @return a FailedEvaluator.
   */
  private synchronized void onEvaluatorFailure(final EvaluatorHeartbeat heartbeat) {
    assert (heartbeat.getState() == EvaluatorState.FAILED);
    // TODO: Fix this.
    final List<FailedContext> failedContexts = null;
    final Optional<FailedTask> failedTask = null;

    final String evaluatorID = heartbeat.getId();
    final String shortMessage;
    final Optional<String> description;
    final Optional<Throwable> cause;
    final Optional<byte[]> serializedException;

    if (heartbeat.getErrorMessage().isPresent()) {
      final ErrorMessage errorMessage = heartbeat.getErrorMessage().get();
      if (errorMessage.getType() == EvaluatorType.JVM && errorMessage.getSerializedException().isPresent()) {
        cause = Optional.of(new ObjectSerializableCodec<Throwable>().decode(errorMessage.getSerializedException().get()));
      } else {
        cause = Optional.empty();
      }
      serializedException = errorMessage.getSerializedException();
      shortMessage = errorMessage.getShortMessage();
      description = errorMessage.getLongMessage();
    } else {
      shortMessage = "Unknown error.";
      description = Optional.empty();
      cause = Optional.empty();
      serializedException = Optional.empty();
    }
    final FailedEvaluator failedEvaluator = new FailedEvaluatorImpl(heartbeat.getId(), shortMessage, description, cause, serializedException, failedContexts, failedTask);
    this.messageDispatcher.onEvaluatorFailed(failedEvaluator);
    this.close();
  }

  @Override
  public synchronized void close() {
    if (EvaluatorState.RUNNING == this.evaluatorState) {
      LOG.log(Level.WARNING, "Dirty shutdown of running evaluator id[{0}]", getId());
      try {
        // Killing the evaluator means that it doesn't need to send a confirmation; it just dies.
        final EvaluatorRuntimeProtocol.EvaluatorControlProto evaluatorControlProto =
            EvaluatorRuntimeProtocol.EvaluatorControlProto.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setIdentifier(getId())
                .setKillEvaluator(EvaluatorRuntimeProtocol.KillEvaluatorProto.newBuilder().build())
                .build();
        this.evaluatorControlHandler.onNext(evaluatorControlProto);
      } finally {
        this.evaluatorState = EvaluatorState.KILLED;
      }
    }
  }

  @Override
  public String getId() {
    return this.evaluatorId;
  }
}