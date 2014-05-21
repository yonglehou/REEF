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
import com.microsoft.reef.exception.evaluator.StorageException;
import com.microsoft.reef.io.Accumulator;
import com.microsoft.reef.io.serialization.Codec;

import java.io.*;

final class CodecFileAccumulator<T> implements Accumulator<T> {

  private final Codec<T> codec;
  private final ObjectOutputStream out;

  public CodecFileAccumulator(final Codec<T> codec, final File file) throws IOException {
    this.codec = codec;
    this.out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
  }

  @Override
  public void add(final T datum) throws ServiceException {
    final byte[] buf = codec.encode(datum);
    try {
      this.out.writeInt(buf.length);
      this.out.write(buf);
    } catch (final IOException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void close() throws ServiceException {
    try {
      this.out.writeInt(-1);
      this.out.close();
    } catch (final IOException e) {
      throw new ServiceException(e);
    }
  }
}
