/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.network.nggroup.api;

import com.microsoft.reef.driver.context.ActiveContext;

/**
 * 
 */
public interface GroupCommDriver {

  /**
   * @param string
   * @return
   */
  CommunicationGroup newCommunicationGroup(String string);

  /**
   * @param activeContext
   * @return
   */
  boolean configured(ActiveContext activeContext);

}
