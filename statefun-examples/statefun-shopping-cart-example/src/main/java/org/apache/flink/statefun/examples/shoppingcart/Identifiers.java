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
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;

final class Identifiers {

  private Identifiers() {}

  static final FunctionType CART = new FunctionType("shopping-cart", "cart");

  static final FunctionType INVENTORY = new FunctionType("shopping-cart", "inventory");

  static final IngressIdentifier<ProtobufMessages.AddToCart> ADD_TO_CART =
      new IngressIdentifier<>(ProtobufMessages.AddToCart.class, "shopping-cart", "add-to-cart");

  static final EgressIdentifier<ProtobufMessages.AddToCartResult> ADD_TO_CART_RESULT =
      new EgressIdentifier<>("shopping-cart", "AddToCartResult", ProtobufMessages.AddToCartResult.class);
}
