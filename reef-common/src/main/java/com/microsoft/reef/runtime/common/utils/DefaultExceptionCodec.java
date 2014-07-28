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
package com.microsoft.reef.runtime.common.utils;

import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.util.Optional;
import org.apache.commons.lang.SerializationException;
import org.apache.commons.lang.SerializationUtils;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default implementation of ExceptionCodec that uses Java serialization as its implementation.
 */
@Private
final class DefaultExceptionCodec implements ExceptionCodec {
  private static final Logger LOG = Logger.getLogger(DefaultExceptionCodec.class.getName());

  @Inject
  DefaultExceptionCodec() {
  }

  @Override
  public Optional<Throwable> fromBytes(final byte[] bytes) {
    try {
      return Optional.<Throwable>of((Throwable) SerializationUtils.deserialize(bytes));
    } catch (SerializationException | IllegalArgumentException e) {
      LOG.log(Level.FINE, "Unable to deserialize a Throwable.", e);
      return Optional.empty();
    }
  }

  @Override
  public Optional<Throwable> fromBytes(final Optional<byte[]> bytes) {
    if (bytes.isPresent()) {
      return this.fromBytes(bytes.get());
    } else {
      return Optional.empty();
    }
  }

  @Override
  public byte[] toBytes(final Throwable throwable) {
    return SerializationUtils.serialize(throwable);
  }
}
