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

package com.microsoft.reef.webserver;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Serialize Http Request Response data with Avro.
 */
public class AvroHttpSerializer {

  public static final String JSON_CHARSET = "ISO-8859-1";

  public AvroHttpSerializer() {
  }

  /**
   * Convert from HttpServletRequest to AvroHttpRequest.
   */
  public AvroHttpRequest toAvro(final ParsedHttpRequest parsedRequest) throws ServletException, IOException {
    return AvroHttpRequest.newBuilder()
        .setRequestUrl(parsedRequest.getRequestUrl())
        .setHttpMethod(parsedRequest.getMethod())
        .setQueryString(parsedRequest.getQueryString())
        .setPathInfo(parsedRequest.getPathInfo())
        .setInputStream(ByteBuffer.wrap(parsedRequest.getInputStream()))
        .build();
  }

  /**
   * Convert AvroHttpRequest to JSON String.
   */
  public String toString(final AvroHttpRequest request) {
    final DatumWriter<AvroHttpRequest> configurationWriter = new SpecificDatumWriter<>(AvroHttpRequest.class);
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(AvroHttpRequest.SCHEMA$, out);
      configurationWriter.write(request, encoder);
      encoder.flush();
      return out.toString(JSON_CHARSET);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Convert AvroHttpRequest to bytes.
   */
  public byte[] toBytes(final AvroHttpRequest request) {
    final DatumWriter<AvroHttpRequest> requestWriter = new SpecificDatumWriter<>(AvroHttpRequest.class);
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      requestWriter.write(request, encoder);
      encoder.flush();
      return out.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Convert JSON String to AvroHttpRequest.
   */
  public AvroHttpRequest fromString(final String jasonStr) {
    try {
      final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(AvroHttpRequest.getClassSchema(), jasonStr);
      final SpecificDatumReader<AvroHttpRequest> reader = new SpecificDatumReader<>(AvroHttpRequest.class);
      return reader.read(null, decoder);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Convert bytes to AvroHttpRequest.
   */
  public AvroHttpRequest fromBytes(final byte[] theBytes) {
    try {
      final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(theBytes, null);
      final SpecificDatumReader<AvroHttpRequest> reader = new SpecificDatumReader<>(AvroHttpRequest.class);
      return reader.read(null, decoder);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
