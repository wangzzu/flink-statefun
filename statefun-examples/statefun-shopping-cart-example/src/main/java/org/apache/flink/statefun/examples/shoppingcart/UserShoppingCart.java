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
import org.apache.flink.statefun.examples.shoppingcart.generated.ProtobufMessages.AddToCartResult.Type;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedTable;

final class UserShoppingCart implements StatefulFunction {

  @Persisted
  private final PersistedTable<String, Integer> cartItems =
      PersistedTable.of("cart-items", String.class, Integer.class);

  @Override
  public void invoke(Context context, Object input) {
    if (input instanceof ProtobufMessages.AddToCart) {
      ProtobufMessages.AddToCart addToCart = (ProtobufMessages.AddToCart) input;
      ProtobufMessages.RequestItem request =
          ProtobufMessages.RequestItem.newBuilder().setQuantity(addToCart.getQuantity()).build();
      Address address = new Address(Identifiers.INVENTORY, addToCart.getItemId());
      context.send(address, request);
    }

    if (input instanceof ProtobufMessages.ItemReserved) {
      ProtobufMessages.ItemReserved itemReserved = (ProtobufMessages.ItemReserved) input;

      cartItems.set(itemReserved.getId(), itemReserved.getQuantity());

      ProtobufMessages.AddToCartResult addToCartResult =
          ProtobufMessages.AddToCartResult.newBuilder()
              .setType(Type.SUCCESS)
              .setItemId(itemReserved.getId())
              .setQuantity(itemReserved.getQuantity())
              .build();

      context.send(Identifiers.ADD_TO_CART_RESULT, addToCartResult);
    }

    if (input instanceof ProtobufMessages.ItemUnavailable) {
      ProtobufMessages.ItemUnavailable itemUnavailable = (ProtobufMessages.ItemUnavailable) input;

      ProtobufMessages.AddToCartResult addToCartResult =
          ProtobufMessages.AddToCartResult.newBuilder()
              .setType(Type.FAIL)
              .setItemId(itemUnavailable.getId())
              .setQuantity(itemUnavailable.getQuantity())
              .build();

      context.send(Identifiers.ADD_TO_CART_RESULT, addToCartResult);
    }
  }
}
