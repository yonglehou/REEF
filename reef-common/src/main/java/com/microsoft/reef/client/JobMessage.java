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
package com.microsoft.reef.client;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.io.Message;
import com.microsoft.reef.io.naming.Identifiable;

/**
 * A message received by the client from the driver.
 */
@Public
@ClientSide
@Provided
public final class JobMessage implements Message, Identifiable {

  private final String id;
  private final byte[] value;

  /**
   * @param id    the identifier of the sending Job
   * @param value the message
   */
  public JobMessage(final String id, final byte[] value) {
    this.id = id;
    this.value = value;
  }

  /**
   * Get the message sent by the Job.
   *
   * @return the message sent by the Job.
   */
  @Override
  public final byte[] get() {
    return this.value;
  }

  /**
   * Get the Identifier of the sending Job.
   *
   * @return the Identifier of the sending Job.
   */
  @Override
  public final String getId() {
    return this.id;
  }

  @Override
  public String toString() {
    return "JobMessage{" +
        "id='" + id + '\'' +
        ", value.length=" + value.length +
        '}';
  }
}
