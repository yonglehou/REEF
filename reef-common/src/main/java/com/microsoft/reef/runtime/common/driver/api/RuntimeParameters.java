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
package com.microsoft.reef.runtime.common.driver.api;

import com.microsoft.reef.annotations.audience.RuntimeAuthor;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.wake.EventHandler;

/**
 * Driver resourcemanager parameters that resourcemanager implementations use to communicate with
 * the Driver.
 */
@RuntimeAuthor
public final class RuntimeParameters {

  @NamedParameter(doc = "The resource allocation handler that stub runtimes send along allocated resources e.g., containers.")
  public final static class ResourceAllocationHandler implements Name<EventHandler<DriverRuntimeProtocol.ResourceAllocationProto>> {
  }

  @NamedParameter(doc = "The node descriptor handler that stub runtimes send along node information.")
  public final static class NodeDescriptorHandler implements Name<EventHandler<DriverRuntimeProtocol.NodeDescriptorProto>> {
  }

  @NamedParameter(doc = "The resource status handler.")
  public final static class ResourceStatusHandler implements Name<EventHandler<DriverRuntimeProtocol.ResourceStatusProto>> {
  }

  @NamedParameter(doc = "The resourcemanager status handler.")
  public final static class RuntimeStatusHandler implements Name<EventHandler<DriverRuntimeProtocol.RuntimeStatusProto>> {
  }

}
