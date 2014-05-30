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
package com.microsoft.reef.io.network.nggroup.impl;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;

/**
 *
 */
public class ParentNodeStruct extends NodeStructImpl {

  private static final Logger LOG = Logger.getLogger(ParentNodeStruct.class.getName());


  public ParentNodeStruct(final String id) {
    super(id);
  }

  @Override
  public boolean checkDead(final GroupCommMessage gcm)  {
    if (gcm.getType() == Type.ParentDead) {
      LOG.log(Level.WARNING, "\t\tGot parent dead msg from driver. Terminating wait and returning null");
      return true;
    }
    return false;
  }

}
