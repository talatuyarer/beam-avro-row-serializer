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
package com.uyarer.beam.serdes.examples;

import com.uyarer.beam.serdes.RowDeserializer;
import com.uyarer.beam.serdes.SerDesRegistry;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

public class AvroBytesToRowConverter extends DoFn<byte[], Row> {
  
  private SerDesRegistry registry;
  
  @Setup
  public void setup() {
    //Create Regisrty in Setup
    registry = SerDesRegistry.getDefaultInstance();
  }
  
  @ProcessElement
  public void processElement(ProcessContext c) throws IOException {
    // Get avro byte record
    byte[] record = c.element();
    
    //Read schema
    Schema writerSchema = null;
    Schema readerSchema = null;

    //Create Avro decoder 
    Decoder decoder = DecoderFactory.get().binaryDecoder(record, null);
    //Get Deserializer
    RowDeserializer<Row> deserializer = registry.getRowDeserializer(writerSchema, readerSchema);
    //Deserialize Avro to Row
    Row row = deserializer.deserialize(decoder);
    c.output(row);
  }
}