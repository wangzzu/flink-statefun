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
package org.apache.flink.statefun.examples.shoppingcart;

import org.apache.flink.statefun.examples.shoppingcart.generated.ProtobufMessages;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedValue;

final class Inventory implements StatefulFunction {

  @Persisted
  private final PersistedValue<Integer> reserveNum = PersistedValue.of("reserveNum", Integer.class);

  @Persisted
  private final PersistedValue<Integer> stockNum = PersistedValue.of("stockNum", Integer.class);

  @Override
  public void invoke(Context context, Object message) {
    if (message instanceof ProtobufMessages.RequestItem) {
      int requestedAmount = ((ProtobufMessages.RequestItem) message).getQuantity();
      if (stockNum.getOrDefault(0) >= requestedAmount ) {
        stockNum.set(stockNum.getOrDefault(0) - requestedAmount);
        reserveNum.set(reserveNum.getOrDefault(0) + requestedAmount);

        ProtobufMessages.ItemReserved.Builder itemReserved =
            ProtobufMessages.ItemReserved.newBuilder().setQuantity(reserveNum.getOrDefault(0));

        context.send(context.caller(), itemReserved);
      } else {
        ProtobufMessages.ItemUnavailable.Builder unavailability =
            ProtobufMessages.ItemUnavailable.newBuilder().setQuantity(requestedAmount);
        context.send(context.caller(), unavailability);
      }
    }
  }
}
