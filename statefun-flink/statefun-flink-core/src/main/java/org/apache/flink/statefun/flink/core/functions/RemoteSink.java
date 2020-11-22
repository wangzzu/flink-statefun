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
package org.apache.flink.statefun.flink.core.functions;

import java.util.Objects;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

final class RemoteSink {
  private final Output<StreamRecord<Message>> output;
  private final StreamRecord<Message> record;

  RemoteSink(Output<StreamRecord<Message>> output) {
    this.output = Objects.requireNonNull(output);
    this.record = new StreamRecord<>(null);
  }

  // note: 通过 flink 发送出去，这里是发送到下游的 feedback 节点
  void accept(Message envelope) {
    Objects.requireNonNull(envelope);
    output.collect(record.replace(envelope));
  }
}
