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

import java.util.List;

import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;

/**
 *
 */
public interface OperatorTopology {

  /**
   * @param msg
   */
  void handle(GroupCommMessage msg);


  /**
   * @param data
   * @param msgType
   */
  void sendToChildren(byte[] data, Type msgType);


  /**
   * @return
   */
  byte[] recvFromParent();


  /**
   * @return
   */
  List<byte[]> recvFromChildren();


  /**
   * @return
   */
  String getSelfId();


  /**
   * @param encode
   * @param reduce
   */
  void sendToParent(byte[] encode, Type reduce);


  /**
   *
   */
  void initialize();

}
