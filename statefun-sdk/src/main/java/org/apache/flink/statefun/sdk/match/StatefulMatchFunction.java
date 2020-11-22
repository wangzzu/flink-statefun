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
package org.apache.flink.statefun.sdk.match;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;

/**
 * A {@link StatefulMatchFunction} is an utility {@link StatefulFunction} that supports pattern
 * matching on function inputs to decide how the inputs should be processed.
 *
 * <p>Please see {@link MatchBinder} for the supported types of pattern matching.
 *
 * @see MatchBinder
 */
// note: 处理多种 msg 类型时，可以使用这个 function，如果输入 msg 类型时固定，直接继承 StatefulFunction 即可
public abstract class StatefulMatchFunction implements StatefulFunction {

  private boolean setup = false;

  private MatchBinder matcher = new MatchBinder();

  /**
   * Configures the patterns to match for the function's inputs.
   * note： 为不同的 msg 类型配置不同的处理方法
   *
   * @param binder a {@link MatchBinder} to bind patterns on.
   */
  public abstract void configure(MatchBinder binder);

  // note: 这里会根据 msg 类型选择对应的处理方法来处理
  @Override
  public final void invoke(Context context, Object input) {
    ensureInitialized();
    matcher.invoke(context, input);
  }

  private void ensureInitialized() {
    if (!setup) {
      setup = true;
      configure(matcher);
    }
  }
}
