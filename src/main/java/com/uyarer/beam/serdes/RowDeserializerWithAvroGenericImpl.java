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

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;

public class RowDeserializerWithAvroGenericImpl<V> implements RowDeserializer<V> {

  private final GenericDatumReader<V> datumReader;

  public RowDeserializerWithAvroGenericImpl(Schema writerSchema, Schema readerSchema) {
    this.datumReader = new DummyRowDatumReader<>(
        writerSchema, readerSchema);
  }

  @Override
  public V deserialize(V reuse, Decoder d) throws IOException {
    return datumReader.read(reuse, d);
  }
}