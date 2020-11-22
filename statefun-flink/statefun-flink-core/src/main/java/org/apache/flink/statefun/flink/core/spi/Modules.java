/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.statefun.flink.core.spi;

import java.util.*;
import org.apache.flink.statefun.flink.common.SetContextClassLoader;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.jsonmodule.JsonServiceLoader;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.flink.io.spi.FlinkIoModule;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

public final class Modules {
  private final List<FlinkIoModule> ioModules;
  // note: StatefulFunctionModule list
  private final List<StatefulFunctionModule> statefulFunctionModules;

  private Modules(
      List<FlinkIoModule> ioModules, List<StatefulFunctionModule> statefulFunctionModules) {
    this.ioModules = ioModules;
    this.statefulFunctionModules = statefulFunctionModules;
  }

  // note: load modules
  public static Modules loadFromClassPath() {
    List<StatefulFunctionModule> statefulFunctionModules = new ArrayList<>();
    List<FlinkIoModule> ioModules = new ArrayList<>();

    // note: ServiceLoader.load Java SPI
    for (StatefulFunctionModule provider : ServiceLoader.load(StatefulFunctionModule.class)) {
      statefulFunctionModules.add(provider);
    }
    // note: Module JSON File
    for (StatefulFunctionModule provider : JsonServiceLoader.load()) {
      statefulFunctionModules.add(provider);
    }
    // note: StateFun distribute jar 包会自动创建 META-INF/services/org.apache.flink.statefun.flink.io.spi.FlinkIoModule 文件
    // note: 这个文件包含 FlinkIoModule 的实现对象
    for (FlinkIoModule provider : ServiceLoader.load(FlinkIoModule.class)) {
      ioModules.add(provider);
    }
    return new Modules(ioModules, statefulFunctionModules);
  }

  // note: create StateFun Universe by module setting
  public StatefulFunctionsUniverse createStatefulFunctionsUniverse(
      StatefulFunctionsConfig configuration) {
    MessageFactoryType factoryType = configuration.getFactoryType();

    // note: 所有的 ioModules 及 statefulFunctionModules 都会 bind 到这个 universe 上
    StatefulFunctionsUniverse universe = new StatefulFunctionsUniverse(factoryType);

    final Map<String, String> globalConfiguration = configuration.getGlobalConfigurations();

    for (FlinkIoModule module : ioModules) {
      try (SetContextClassLoader ignored = new SetContextClassLoader(module)) {
        module.configure(globalConfiguration, universe);
      }
    }
    for (StatefulFunctionModule module : statefulFunctionModules) {
      try (SetContextClassLoader ignored = new SetContextClassLoader(module)) {
        module.configure(globalConfiguration, universe);
      }
    }

    return universe;
  }
}
