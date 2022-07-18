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

package com.uyarer.beam.serdes.utils;

import com.uyarer.beam.serdes.DummyRowDatumReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.values.Row;

public final class SerDesTestUtils {

  public static final String NAMESPACE = "firewall";

  private SerDesTestUtils() {
  }

  /**
   * Read AVRO file into GenericRecord.
   *
   * @param file   input AVRO file.
   * @param schema AVRO schema.
   */
  public static GenericRecord readAvroFileWithGenericReader(
      File file, Schema schema) throws IOException {
    // Deserialize users from disk
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file,
        datumReader);
    GenericRecord record = null;
    while (dataFileReader.hasNext()) {
      // Reuse record object by passing it to next(). This saves us from
      // allocating and garbage collecting many objects for files with
      // many items.
      record = dataFileReader.next(record);
    }
    return record;
  }

  /**
   * Read AVRO file into {@link Row}.
   *
   * @param file   input AVRO file.
   * @param schema AVRO schema.
   */
  public static Row readAvroFileWithDummyReader(File file, Schema schema) throws IOException {
    // Deserialize users from disk
    DatumReader<Row> datumReader = new DummyRowDatumReader<>(schema, schema);
    DataFileReader<Row> dataFileReader = new DataFileReader<Row>(file, datumReader);
    Row record = null;
    while (dataFileReader.hasNext()) {
      // Reuse record object by passing it to next(). This saves us from
      // allocating and garbage collecting many objects for files with
      // many items.
      record = dataFileReader.next(record);
    }
    return record;
  }

  /**
   * Generates Record type.
   *
   * @param name   Record name
   * @param fields Record Fields
   * @return
   */
  public static Schema createRecord(String name, Schema.Field... fields) {
    Schema schema = Schema.createRecord(name, name, NAMESPACE, false);
    schema.setFields(Arrays.asList(fields));

    return schema;
  }

  /**
   * Generates Field.
   *
   * @param name   Field name
   * @param schema Field schema
   * @return
   */
  public static Schema.Field createField(String name, Schema schema) {
    return new Schema.Field(name, schema, "", (Object) null, Schema.Field.Order.ASCENDING);
  }

  /**
   * Creates Union Field from given schemas.
   *
   * @param name    Field Name
   * @param schemas Schemas
   * @return
   */
  public static Schema.Field createUnionField(String name, Schema... schemas) {
    List<Schema> typeList = new ArrayList<>();
    typeList.add(Schema.create(Schema.Type.NULL));
    typeList.addAll(Arrays.asList(schemas));

    Schema unionSchema = Schema.createUnion(typeList);
    return new Schema.Field(name, unionSchema, null, Schema.Field.Order.ASCENDING);
  }

  public static Schema.Field createPrimitiveFieldSchema(String name, Schema.Type type) {
    return new Schema.Field(name, Schema.create(type), null, (Object) null);
  }

  /**
   * Creates Union Field from given types.
   *
   * @param name  Field Name
   * @param types Types in Union
   * @return
   */
  public static Schema.Field createPrimitiveUnionFieldSchema(String name, Schema.Type... types) {
    List<Schema> typeList = new ArrayList<>();
    typeList.add(Schema.create(Schema.Type.NULL));
    typeList.addAll(Arrays.stream(types).map(Schema::create).collect(Collectors.toList()));

    Schema unionSchema = Schema.createUnion(typeList);
    return new Schema.Field(name, unionSchema, null, (Object) null,
        Schema.Field.Order.ASCENDING);
  }

  /**
   * Creates Array Field for given elementType.
   *
   * @param name        Field Name
   * @param elementType Array element type
   * @param aliases     Aliases for array field.
   * @return
   */
  public static Schema.Field createArrayFieldSchema(String name, Schema elementType,
      String... aliases) {
    return addAliases(
        new Schema.Field(name, Schema.createArray(elementType), null, (Object) null,
            Schema.Field.Order.ASCENDING), aliases);
  }

  public static Schema.Field createMapFieldSchema(String name, Schema valueType,
      String... aliases) {
    return addAliases(new Schema.Field(name, Schema.createMap(valueType), null, (Object) null,
        Schema.Field.Order.ASCENDING), aliases);
  }

  public static Schema createFixedSchema(String name, int size) {
    return Schema.createFixed(name, "", NAMESPACE, size);
  }

  public static Schema createEnumSchema(String name, String[] ordinals) {
    return Schema.createEnum(name, "", NAMESPACE, Arrays.asList(ordinals));
  }

  /**
   * Creates Union Schema from given Schemas.
   *
   * @param schemas Avro Schemas
   * @return
   */
  public static Schema createUnionSchema(Schema... schemas) {
    List<Schema> typeList = new ArrayList<>();
    typeList.add(Schema.create(Schema.Type.NULL));
    typeList.addAll(Arrays.asList(schemas));

    return Schema.createUnion(typeList);
  }

  /**
   * Add aliases to given Field.
   *
   * @param field   Field
   * @param aliases Aliases
   * @return
   */
  public static Schema.Field addAliases(Schema.Field field, String... aliases) {
    if (aliases != null) {
      Arrays.asList(aliases).forEach(field::addAlias);
    }

    return field;
  }

  /**
   * Create Binary Encoder for given Avro Record.
   *
   * @param data Avro Record
   * @return
   */
  public static Decoder serializeAvro(GenericRecord data) {
    return serializeAvro(data, data.getSchema());
  }

  /**
   * Create Binary Encoder for given Avro Record.
   *
   * @param data   Avro Record
   * @param schema Avro Schema
   * @return
   */
  public static Decoder serializeAvro(GenericRecord data, Schema schema) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(baos, null);

    try {
      GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
      writer.write(data, binaryEncoder);
      binaryEncoder.flush();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return DecoderFactory.get().binaryDecoder(baos.toByteArray(), null);
  }

  /**
   * Reads sample logs from resource folder for given schema and log name.
   *
   * @param logName File needs to continas logName
   * @param schema  Avro schema of file.
   * @return
   */
  public static List<GenericRecord> readSampleLogs(String logName, Schema schema)
      throws IOException {
    File dir = new File(SerDesTestUtils.class.getClassLoader().getResource("data").getPath());
    List<GenericRecord> expectedOutput = new ArrayList<>();
    for (File file : dir.listFiles()) {
      if (file.getName().contains(logName)) {
        GenericRecord record = readAvroFileWithGenericReader(file, schema);
        expectedOutput.add(record);
      }
    }

    return expectedOutput;
  }
}
