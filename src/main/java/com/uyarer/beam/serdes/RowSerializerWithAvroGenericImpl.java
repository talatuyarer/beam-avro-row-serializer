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
import java.io.OutputStream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.values.Row;

public class RowSerializerWithAvroGenericImpl<V> implements RowSerializer<V> {

  private final DummyRowDatumWriter<GenericRecord> datumWriter;
  private final Schema writerSchema;
  private final Schema unionSchema;

  /**
   * Row Serializer with Generic Implementation.
   *
   * @param writerSchema Avro schema.
   */
  public RowSerializerWithAvroGenericImpl(Schema writerSchema) {
    this.writerSchema = writerSchema;
    this.unionSchema = SchemaBuilder.builder().unionOf()
        .type(writerSchema).endUnion();
    this.datumWriter = new DummyRowDatumWriter<>(unionSchema);
  }


  @Override
  public void serialize(V datum, OutputStream output) throws IOException {
    GenericRecord record = AvroUtils
        .toGenericRecord((Row) datum, writerSchema);

    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
    datumWriter.write(record, encoder);
    encoder.flush();
    output.flush();
  }
}