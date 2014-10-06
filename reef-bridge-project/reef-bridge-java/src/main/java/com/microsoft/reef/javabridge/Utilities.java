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

package com.microsoft.reef.javabridge;

import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.implementation.protobuf.ProtocolBufferClassHierarchy;
import com.microsoft.tang.proto.ClassHierarchyProto;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Utilities {
  public static ClassHierarchy loadClassHierarchy(String classHierarchyFile) {
    Path p = Paths.get(classHierarchyFile);
    if (!Files.exists(p))
    {
      p = Paths.get(System.getProperty("user.dir") + "/reef/global/" + classHierarchyFile);
    }
    if(!Files.exists(p))
    {
      throw new RuntimeException("cannot find file " + p.toAbsolutePath());
    }
    try (final InputStream chin = new FileInputStream(p.toAbsolutePath().toString())) {
      final ClassHierarchyProto.Node root = ClassHierarchyProto.Node.parseFrom(chin);
      final ClassHierarchy ch = new ProtocolBufferClassHierarchy(root);
      return ch;
    } catch (final IOException e) {
      final String message = "Unable to load class hierarchy from " + classHierarchyFile;
      throw new RuntimeException(message, e);
    }
  }

  public static String getEvaluatorDescriptorString(EvaluatorDescriptor evaluatorDescriptor)
  {
    InetSocketAddress socketAddress = evaluatorDescriptor.getNodeDescriptor().getInetSocketAddress();
    return "IP=" + socketAddress.getAddress() + ", Port=" +  socketAddress.getPort() + ", HostName=" + socketAddress.getHostName() + ", Memory=" + evaluatorDescriptor.getMemory() + ", Core=" + evaluatorDescriptor.getNumberOfCores();
  }
}
