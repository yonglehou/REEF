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
package com.microsoft.reef.examples.nggroup.bgd.data.parser;

import com.microsoft.reef.examples.nggroup.bgd.data.Example;
import com.microsoft.reef.examples.nggroup.bgd.data.SparseExample;
import org.apache.commons.lang.StringUtils;

import javax.inject.Inject;

/**
 * A Parser for SVMLight records
 */
public class SVMLightParser implements Parser<String> {

  @Inject
  public SVMLightParser() {
  }

  @Override
  public Example parse(final String line) {
    final int entriesCount = StringUtils.countMatches(line, ":");
    final int[] indices = new int[entriesCount];
    final double[] values = new double[entriesCount];

    final String[] entries = StringUtils.split(line, ' ');
    double label = Double.parseDouble(entries[0]);

    if (label != 1) {
      label = -1;
    }

    for (int j = 1; j < entries.length; ++j) {
      final String x = entries[j];
      final String[] entity = StringUtils.split(x, ':');
      indices[j - 1] = Integer.parseInt(entity[0]) - 1;
      values[j - 1] = Double.parseDouble(entity[1]);
    }
    return new SparseExample(label, values, indices);
  }
}
