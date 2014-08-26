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
package com.microsoft.reef.io.storage.local;

import com.microsoft.reef.exception.evaluator.ServiceException;
import com.microsoft.reef.exception.evaluator.ServiceRuntimeException;
import com.microsoft.reef.io.Accumulable;
import com.microsoft.reef.io.Accumulator;
import com.microsoft.reef.io.Spool;
import com.microsoft.reef.io.serialization.Deserializer;
import com.microsoft.reef.io.serialization.Serializer;

import java.io.*;
import java.util.ConcurrentModificationException;
import java.util.Iterator;

/**
 * A SpoolFile backed by the filesystem.
 * @param <T>
 */
public final class SerializerFileSpool<T> implements Spool<T> {

  private final File file;
  private final Accumulator<T> accumulator;
  private final Deserializer<T, InputStream> deserializer;
  private boolean canAppend = true;
  private boolean canGetAccumulator = true;

  public SerializerFileSpool(final LocalStorageService service,
      final Serializer<T, OutputStream> out, final Deserializer<T, InputStream> in)
      throws ServiceException {
    this.file = service.getScratchSpace().newFile();
    Accumulable<T> accumulable;
    try {
      accumulable = out.create(new BufferedOutputStream(new FileOutputStream(
          file)));
    } catch (final FileNotFoundException e) {
      throw new IllegalStateException(
          "Unable to create temporary file:" + file, e);
    }
    this.deserializer = in;

    final Accumulator<T> acc = accumulable.accumulator();
    this.accumulator = new Accumulator<T>() {
      @Override
      public void add(final T datum) throws ServiceException {
        if (!canAppend) {
          throw new ConcurrentModificationException(
              "Attempt to append after creating iterator!");
        }
        acc.add(datum);
      }

      @Override
      public void close() throws ServiceException {
        canAppend = false;
        acc.close();
      }
    };
  }

  @Override
  public Iterator<T> iterator() {
    try {
      if (canAppend) {
        throw new IllegalStateException(
            "Need to call close() on accumulator before calling iterator()!");
      }
      return deserializer.create(
          new BufferedInputStream(new FileInputStream(file))).iterator();
    } catch (final IOException e) {
      throw new ServiceRuntimeException(e);
    }
  }

  @Override
  public Accumulator<T> accumulator() {
    if (!canGetAccumulator) {
      throw new UnsupportedOperationException("Can only getAccumulator() once!");
    }
    canGetAccumulator = false;
    return this.accumulator;
  }
}
