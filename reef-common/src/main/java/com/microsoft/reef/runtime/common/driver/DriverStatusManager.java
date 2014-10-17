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
package com.microsoft.reef.runtime.common.driver;

import com.google.protobuf.ByteString;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import com.microsoft.reef.runtime.common.driver.client.ClientConnection;
import com.microsoft.reef.runtime.common.utils.ExceptionCodec;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.time.Clock;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages the Driver's status.
 */
public final class DriverStatusManager {
  private static final Logger LOG = Logger.getLogger(DriverStatusManager.class.getName());
  private final Clock clock;
  private final ClientConnection clientConnection;
  private final String jobIdentifier;
  private final ExceptionCodec exceptionCodec;
  private DriverStatus driverStatus = DriverStatus.PRE_INIT;
  private Optional<Throwable> shutdownCause = Optional.empty();
  private boolean driverTerminationHasBeenCommunicatedToClient = false;
  private boolean restartCompleted = false;
  private int numPreviousContainers = -1;
  private int numRecoveredContainers = 0;


  /**
   * @param clock
   * @param clientConnection
   * @param jobIdentifier
   * @param exceptionCodec
   */
  @Inject
  DriverStatusManager(final Clock clock,
                      final ClientConnection clientConnection,
                      final @Parameter(AbstractDriverRuntimeConfiguration.JobIdentifier.class) String jobIdentifier,
                      final ExceptionCodec exceptionCodec) {
    LOG.entering(DriverStatusManager.class.getCanonicalName(), "<init>");
    this.clock = clock;
    this.clientConnection = clientConnection;
    this.jobIdentifier = jobIdentifier;
    this.exceptionCodec = exceptionCodec;
    LOG.log(Level.FINE, "Instantiated 'DriverStatusManager'");
    LOG.exiting(DriverStatusManager.class.getCanonicalName(), "<init>");
  }

  /**
   * Changes the driver status to INIT and sends message to the client about the transition.
   */
  public synchronized void onInit() {
    LOG.entering(DriverStatusManager.class.getCanonicalName(), "onInit");
    this.clientConnection.send(this.getInitMessage());
    this.setStatus(DriverStatus.INIT);
    LOG.exiting(DriverStatusManager.class.getCanonicalName(), "onInit");
  }

  /**
   * Changes the driver status to RUNNING and sends message to the client about the transition.
   * If the driver is in status 'PRE_INIT', this first calls onInit();
   */
  public synchronized void onRunning() {
    LOG.entering(DriverStatusManager.class.getCanonicalName(), "onRunning");
    if (this.driverStatus.equals(DriverStatus.PRE_INIT)) {
      this.onInit();
    }
    this.clientConnection.send(this.getRunningMessage());
    this.setStatus(DriverStatus.RUNNING);
    LOG.exiting(DriverStatusManager.class.getCanonicalName(), "onRunning");
  }

  /**
   * End the Driver with an exception.
   *
   * @param exception
   */
  public synchronized void onError(final Throwable exception) {
    LOG.entering(DriverStatusManager.class.getCanonicalName(), "onError", new Object[]{exception});
    LOG.log(Level.WARNING, "Shutting down the Driver with an exception: ", exception);
    this.shutdownCause = Optional.of(exception);
    this.clock.stop();
    this.setStatus(DriverStatus.FAILING);
    LOG.exiting(DriverStatusManager.class.getCanonicalName(), "onError", new Object[]{exception});
  }

  /**
   * Perform a clean shutdown of the Driver.
   */
  public synchronized void onComplete() {
    LOG.entering(DriverStatusManager.class.getCanonicalName(), "onComplete");
    LOG.log(Level.INFO, "Clean shutdown of the Driver.");
    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "Callstack: ", new Exception());
    }
    this.clock.stop();
    this.setStatus(DriverStatus.SHUTTING_DOWN);
    LOG.exiting(DriverStatusManager.class.getCanonicalName(), "onComplete");
  }

  /**
   * Sends the final message to the Driver. This is used by DriverRuntimeStopHandler.onNext().
   *
   * @param exception
   */
  public synchronized void sendJobEndingMessageToClient(final Optional<Throwable> exception) {
    if (this.isNotShuttingDownOrFailing()) {
      LOG.log(Level.SEVERE, "Sending message in a state different that SHUTTING_DOWN or FAILING. This is likely a illegal call to clock.close() at play. Current state: " + this.driverStatus);
    }
    if (this.driverTerminationHasBeenCommunicatedToClient) {
      LOG.log(Level.SEVERE, ".sendJobEndingMessageToClient() called twice. Ignoring the second call");
    } else {
      { // Log the shutdown situation
        if (this.shutdownCause.isPresent()) {
          LOG.log(Level.WARNING, "Sending message about an unclean driver shutdown.", this.shutdownCause.get());
        }
        if (exception.isPresent()) {
          LOG.log(Level.WARNING, "There was an exception during clock.close().", exception.get());
        }
        if (this.shutdownCause.isPresent() && exception.isPresent()) {
          LOG.log(Level.WARNING, "The driver is shutdown because of an exception (see above) and there was an exception during clock.close(). Only the first exception will be sent to the client");
        }
      }
      if (this.shutdownCause.isPresent()) {
        // Send the earlier exception, if there was one
        this.clientConnection.send(getJobEndingMessage(this.shutdownCause));
      } else {
        // Send the exception passed, if there was one.
        this.clientConnection.send(getJobEndingMessage(exception));
      }
      this.driverTerminationHasBeenCommunicatedToClient = true;
    }
  }

  /**
   * Indicate that the Driver restart is complete. It is meant to be called exactly once during a restart and never
   * during the ininital launch of a Driver.
   */
  public synchronized void setRestartCompleted() {
    if (!this.isDriverRestart()) {
      throw new IllegalStateException("setRestartCompleted() called in a Driver that is not, in fact, restarted.");
    } else if (this.restartCompleted) {
      LOG.log(Level.WARNING, "Calling setRestartCompleted more than once.");
    } else {
      this.restartCompleted = true;
    }
  }

  /**
   * @return the number of Evaluators expected to check in from a previous run.
   */
  public synchronized int getNumPreviousContainers() {
    return this.numPreviousContainers;
  }

  /**
   * Set the number of containers to expect still active from a previous execution of the Driver in a restart situation.
   * To be called exactly once during a driver restart.
   *
   * @param num
   */
  public synchronized void setNumPreviousContainers(final int num) {
    if (this.numPreviousContainers >= 0) {
      throw new IllegalStateException("Attempting to set the number of expected containers left from a previous container more than once.");
    } else {
      this.numPreviousContainers = num;
    }
  }

  /**
   * @return the number of Evaluators from a previous Driver that have checked in with the Driver in a restart situation.
   */
  public synchronized int getNumRecoveredContainers() {
    return this.numRecoveredContainers;
  }

  /**
   * Indicate that this Driver has re-established the connection with one more Evaluator of a previous run.
   */
  public synchronized void oneContainerRecovered() {
    this.numRecoveredContainers += 1;
    if (this.numRecoveredContainers > this.numPreviousContainers) {
      throw new IllegalStateException("Reconnected to" +
          this.numRecoveredContainers +
          "Evaluators while only expecting " +
          this.numPreviousContainers);
    }
  }

  /**
   * @return true if the Driver is a restarted driver of an earlier attempt.
   */
  private synchronized boolean isDriverRestart() {
    return this.getNumPreviousContainers() > 0;
  }

  private synchronized boolean isShuttingDownOrFailing() {
    return DriverStatus.SHUTTING_DOWN.equals(this.driverStatus)
        || DriverStatus.FAILING.equals(this.driverStatus);
  }

  private synchronized boolean isNotShuttingDownOrFailing() {
    return !isShuttingDownOrFailing();
  }

  /**
   * Helper method to set the status. This also checks whether the transition from the current status to the new one is
   * legal.
   *
   * @param newStatus
   */
  private synchronized void setStatus(final DriverStatus newStatus) {
    if (isLegalTransition(this.driverStatus, newStatus)) {
      this.driverStatus = newStatus;
    } else {
      LOG.log(Level.WARNING, "Illegal state transiton: '" + this.driverStatus + "'->'" + newStatus + "'");
    }
  }


  /**
   * @param exception the exception that ended the Driver, if any.
   * @return message to be sent to the client at the end of the job.
   */
  private synchronized ReefServiceProtos.JobStatusProto getJobEndingMessage(final Optional<Throwable> exception) {
    final ReefServiceProtos.JobStatusProto message;
    if (exception.isPresent()) {
      message = ReefServiceProtos.JobStatusProto.newBuilder()
          .setIdentifier(this.jobIdentifier)
          .setState(ReefServiceProtos.State.FAILED)
          .setException(ByteString.copyFrom(this.exceptionCodec.toBytes(exception.get())))
          .build();
    } else {
      message = ReefServiceProtos.JobStatusProto.newBuilder()
          .setIdentifier(this.jobIdentifier)
          .setState(ReefServiceProtos.State.DONE)
          .build();
    }
    return message;
  }

  /**
   * @return The message to be sent through the ClientConnection when in state INIT.
   */
  private synchronized ReefServiceProtos.JobStatusProto getInitMessage() {
    return ReefServiceProtos.JobStatusProto.newBuilder()
        .setIdentifier(this.jobIdentifier)
        .setState(ReefServiceProtos.State.INIT)
        .build();
  }

  /**
   * @return The message to be sent through the ClientConnection when in state RUNNING.
   */
  private synchronized ReefServiceProtos.JobStatusProto getRunningMessage() {
    return ReefServiceProtos.JobStatusProto.newBuilder()
        .setIdentifier(this.jobIdentifier)
        .setState(ReefServiceProtos.State.RUNNING)
        .build();
  }

  /**
   * Check whether a state transition 'from->to' is legal.
   *
   * @param from
   * @param to
   * @return
   */
  private static boolean isLegalTransition(final DriverStatus from, final DriverStatus to) {
    switch (from) {
      case PRE_INIT:
        switch (to) {
          case INIT:
            return true;
          default:
            return false;
        }
      case INIT:
        switch (to) {
          case RUNNING:
            return true;
          default:
            return false;
        }
      case RUNNING:
        switch (to) {
          case SHUTTING_DOWN:
          case FAILING:
            return true;
          default:
            return false;
        }
      case FAILING:
      case SHUTTING_DOWN:
        return false;
      default:
        throw new IllegalStateException("Unknown input state: " + from);
    }
  }
}
