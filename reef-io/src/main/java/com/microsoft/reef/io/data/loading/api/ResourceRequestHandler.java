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
package com.microsoft.reef.io.data.loading.api;

import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.wake.EventHandler;

import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ResourceRequestHandler implements EventHandler<EvaluatorRequest> {

  private static final Logger LOG = Logger.getLogger(ResourceRequestHandler.class.getName());

  private final EvaluatorRequestor requestor;

  private CountDownLatch resourceRequestGate = new CountDownLatch(1);

  public ResourceRequestHandler(final EvaluatorRequestor requestor) {
    this.requestor = requestor;
  }

  public void releaseResourceRequestGate() {
    LOG.log(Level.FINE, "Releasing Gate");
    this.resourceRequestGate.countDown();
  }

  @Override
  public void onNext(final EvaluatorRequest request) {
    try {

      LOG.log(Level.FINE,
          "Processing a request with count: {0} - Waiting for gate to be released",
          request.getNumber());

      this.resourceRequestGate.await();

      LOG.log(Level.FINE, "Gate released. Submitting request: {0}", request);
      this.resourceRequestGate = new CountDownLatch(1);
      this.requestor.submit(request);

    } catch (final InterruptedException ex) {
      LOG.log(Level.FINEST, "Interrupted", ex);
    }
  }
}
