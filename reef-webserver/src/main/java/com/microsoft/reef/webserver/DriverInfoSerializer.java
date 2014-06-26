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

import com.microsoft.tang.annotations.DefaultImplementation;

/**
 * Interface for DriverInfoSerializer
 */
@DefaultImplementation(AvroDriverInfoSerializer.class)
public interface DriverInfoSerializer {
  /**
   * Build AvroDriverInfo object from raw data
   *
   * @param id
   * @param startTime
   * @return AvroDriverInfo object
   */
  public AvroDriverInfo toAvro(final String id, final String startTime);

  /**
   * Convert AvroDriverInfo object to JSon string
   *
   * @param avroDriverInfo
   * @return
   */
  public String toString(final AvroDriverInfo avroDriverInfo);
}
