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

import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.driver.catalog.RackDescriptor;
import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.driver.evaluator.EvaluatorType;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import org.junit.Assert;
import org.junit.Test;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test Avro Serializer for http schema
 */
public class TestAvroSerializerForHttp {

  @Test
  public void DriverInfoSerializerInjectionTest() {
    try {
      final DriverInfoSerializer serializer = Tang.Factory.getTang().newInjector().getInstance(DriverInfoSerializer.class);
      final AvroDriverInfo driverInfo = serializer.toAvro("abc", "xxxxxx");
      final String driverInfoString = serializer.toString(driverInfo);
      Assert.assertEquals(driverInfoString, "{\"remoteId\":\"abc\",\"startTime\":\"xxxxxx\"}");
    } catch (final InjectionException e) {
      Assert.fail("Not able to inject DriverInfoSerializer");
    }
  }

  @Test
  public void EvaluatorInfoSerializerInjectionTest() {
    try {
      final EvaluatorInfoSerializer serializer = Tang.Factory.getTang().newInjector().getInstance(EvaluatorInfoSerializer.class);

      final List<String> ids = new ArrayList<>();
      ids.add("abc");
      final EvaluatorDescriptor evaluatorDescriptor = Tang.Factory.getTang().newInjector(EvaluatorDescriptorConfig.CONF.build()).getInstance(EvaluatorDescriptor.class);
      final Map<String, EvaluatorDescriptor> data = new HashMap<>();
      data.put("abc", evaluatorDescriptor);

      final AvroEvaluatorsInfo evaluatorInfo = serializer.toAvro(ids, data);
      final String evaluatorInfoString = serializer.toString(evaluatorInfo);
      Assert.assertEquals(evaluatorInfoString, "{\"evaluatorsInfo\":[{\"evaluatorId\":\"abc\",\"nodeId\":\"\",\"nodeName\":\"mock\",\"memory\":64,\"type\":\"CLR\",\"internetAddress\":\"\"}]}");
    } catch (final InjectionException e) {
      Assert.fail("Not able to inject EvaluatorInfoSerializer");
    }
  }

  @Test
  public void EvaluatorListSerializerInjectionTest() {
    try {
      final EvaluatorListSerializer serializer = Tang.Factory.getTang().newInjector().getInstance(EvaluatorListSerializer.class);

      final List<String> ids = new ArrayList<>();
      ids.add("abc");
      final EvaluatorDescriptor evaluatorDescriptor = Tang.Factory.getTang().newInjector(EvaluatorDescriptorConfig.CONF.build()).getInstance(EvaluatorDescriptor.class);
      final Map<String, EvaluatorDescriptor> data = new HashMap<>();
      data.put("abc", evaluatorDescriptor);

      final AvroEvaluatorList evaluatorList = serializer.toAvro(data, 1, "xxxxxx");
      final String evaluatorListString = serializer.toString(evaluatorList);
      Assert.assertEquals(evaluatorListString, "{\"evaluators\":[{\"id\":\"abc\",\"name\":\"mock\"}],\"total\":1,\"startTime\":\"xxxxxx\"}");
    } catch (final InjectionException e) {
      Assert.fail("Not able to inject EvaluatorListSerializer");
    }
  }

  public static final class EvaluatorDescriptorConfig extends ConfigurationModuleBuilder {
    static final ConfigurationModule CONF = new EvaluatorDescriptorConfig()
        .bindImplementation(EvaluatorDescriptor.class, EvaluatorDescriptorMock.class)
        .bindImplementation(NodeDescriptor.class, NodeDescriptorMock.class)
        .build();
  }

  static class EvaluatorDescriptorMock implements EvaluatorDescriptor {
    final NodeDescriptor nodeDescriptor;

    @Inject
    EvaluatorDescriptorMock(final NodeDescriptor nodeDescriptor) {
      this.nodeDescriptor = nodeDescriptor;
    }

    /**
     * @return the NodeDescriptor of the node where this Evaluator is running.
     */
    @Override
    public NodeDescriptor getNodeDescriptor() {
      return nodeDescriptor;
    }

    /**
     * @return the type of Evaluator.
     */
    @Override
    public EvaluatorType getType() {
      return EvaluatorType.CLR;
    }

    /**
     * @return the amount of memory allocated to this Evaluator.
     */
    @Override
    public int getMemory() {
      return 64;
    }
  }

  static class NodeDescriptorMock implements NodeDescriptor {
    @Inject
    NodeDescriptorMock() {
    }

    /**
     * Access the inet address of the Evaluator.
     *
     * @return the inet address of the Evaluator.
     */
    @Override
    public InetSocketAddress getInetSocketAddress() {
      return null;
    }

    /**
     * @return the rack descriptor that contains this node
     */
    @Override
    public RackDescriptor getRackDescriptor() {
      return null;
    }

    @Override
    public String getName() {
      return "mock";
    }

    /**
     * Returns an identifier of this object
     *
     * @return an identifier of this object
     */
    @Override
    public String getId() {
      return null;
    }
  }
}
