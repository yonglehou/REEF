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
package com.microsoft.reef.io.network.naming.serialization;

import com.microsoft.io.network.naming.avro.AvroNamingAssignment;
import com.microsoft.io.network.naming.avro.AvroNamingLookupResponse;
import com.microsoft.reef.io.naming.NameAssignment;
import com.microsoft.reef.io.network.naming.NameAssignmentTuple;
import com.microsoft.reef.io.network.naming.exception.NamingRuntimeException;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.remote.Codec;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Naming lookup response codec
 */
public final class NamingLookupResponseCodec implements Codec<NamingLookupResponse> {

  private final IdentifierFactory factory;

  /**
   * Constructs a naming lookup response codec
   *
   * @param factory the identifier factory
   */
  @Inject
  public NamingLookupResponseCodec(final IdentifierFactory factory) {
    this.factory = factory;
  }

  /**
   * Encodes name assignments to bytes
   *
   * @param obj the naming lookup response
   * @return a byte array
   */
  @Override
  public byte[] encode(NamingLookupResponse obj) {
    final List<AvroNamingAssignment> assignments = new ArrayList<>(obj.getNameAssignments().size());
    for (final NameAssignment nameAssignment : obj.getNameAssignments()) {
      assignments.add(AvroNamingAssignment.newBuilder()
          .setId(nameAssignment.getIdentifier().toString())
          .setHost(nameAssignment.getAddress().getHostName())
          .setPort(nameAssignment.getAddress().getPort())
          .build());
    }
    return AvroUtils.toBytes(
        AvroNamingLookupResponse.newBuilder().setTuples(assignments).build(), AvroNamingLookupResponse.class
    );
  }

  /**
   * Decodes bytes to an iterable of name assignments
   *
   * @param buf the byte array
   * @return a naming lookup response
   * @throws NamingRuntimeException
   */
  @Override
  public NamingLookupResponse decode(final byte[] buf) {
    final AvroNamingLookupResponse avroResponse = AvroUtils.fromBytes(buf, AvroNamingLookupResponse.class);
    final List<NameAssignment> nas = new ArrayList<NameAssignment>(avroResponse.getTuples().size());
    for (final AvroNamingAssignment tuple : avroResponse.getTuples()) {
      nas.add(
          new NameAssignmentTuple(
              factory.getNewInstance(tuple.getId().toString()),
              new InetSocketAddress(tuple.getHost().toString(), tuple.getPort())
          )
      );
    }
    return new NamingLookupResponse(nas);
  }

}
