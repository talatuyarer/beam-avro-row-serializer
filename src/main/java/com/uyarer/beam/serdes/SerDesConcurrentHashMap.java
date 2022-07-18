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

package com.uyarer.beam.serdes;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class SerDesConcurrentHashMap<K, V> extends ConcurrentHashMap<K, V> {

  public SerDesConcurrentHashMap() {
    super();
  }

  public SerDesConcurrentHashMap(int initialCapacity) {
    super(initialCapacity);
  }

  /**
   * The native `computeIfAbsent` function implemented in Java could have contention when the value
   * already exists {@link ConcurrentHashMap#computeIfAbsent(Object, Function)}; the contention
   * could become very bad when lots of threads are trying to "computeIfAbsent" on the same key,
   * which is a known Java bug: https://bugs.openjdk.java.net/browse/JDK-8161372
   * {@inheritDoc}
   */
  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    V value = get(key);
    if (value != null) {
      return value;
    }
    return super.computeIfAbsent(key, mappingFunction);
  }
}