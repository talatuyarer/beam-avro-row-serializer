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

import static com.uyarer.beam.serdes.utils.SerDesTestUtils.addAliases;
import static com.uyarer.beam.serdes.utils.SerDesTestUtils.createArrayFieldSchema;
import static com.uyarer.beam.serdes.utils.SerDesTestUtils.createEnumSchema;
import static com.uyarer.beam.serdes.utils.SerDesTestUtils.createField;
import static com.uyarer.beam.serdes.utils.SerDesTestUtils.createFixedSchema;
import static com.uyarer.beam.serdes.utils.SerDesTestUtils.createMapFieldSchema;
import static com.uyarer.beam.serdes.utils.SerDesTestUtils.createPrimitiveFieldSchema;
import static com.uyarer.beam.serdes.utils.SerDesTestUtils.createPrimitiveUnionFieldSchema;
import static com.uyarer.beam.serdes.utils.SerDesTestUtils.createRecord;
import static com.uyarer.beam.serdes.utils.SerDesTestUtils.createUnionField;
import static com.uyarer.beam.serdes.utils.SerDesTestUtils.createUnionSchema;
import static com.uyarer.beam.serdes.utils.SerDesTestUtils.serializeAvro;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.beam.sdk.values.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class RowDeserializerCodeGeneratorTests {

  private File tempDir;
  private ClassLoader classLoader;

  /**
   * {@inheritDoc}
   */
  @BeforeEach
  public void prepare() throws Exception {
    Path tempPath = Files.createTempDirectory("generated");
    tempDir = tempPath.toFile();

    classLoader = URLClassLoader.newInstance(new URL[]{tempDir.toURI().toURL()},
        RowDeserializerCodeGeneratorTests.class.getClassLoader());
  }

  @Test
  public void testPrimitivesTypes() {
    // Prepare a Generic Record
    Schema javaLangStringSchema = Schema.create(Schema.Type.STRING);
    GenericData.setStringType(javaLangStringSchema, GenericData.StringType.String);
    Schema recordSchema = createRecord("testRecord",
        createField("testInt", Schema.create(Schema.Type.INT)),
        createPrimitiveUnionFieldSchema("testIntUnion", Schema.Type.INT),
        createField("testString", Schema.create(Schema.Type.STRING)),
        createPrimitiveUnionFieldSchema("testStringUnion", Schema.Type.STRING),
        createField("testJavaString", javaLangStringSchema),
        createUnionField("testJavaStringUnion", javaLangStringSchema),
        createField("testLong", Schema.create(Schema.Type.LONG)),
        createPrimitiveUnionFieldSchema("testLongUnion", Schema.Type.LONG),
        createField("testDouble", Schema.create(Schema.Type.DOUBLE)),
        createPrimitiveUnionFieldSchema("testDoubleUnion", Schema.Type.DOUBLE),
        createField("testFloat", Schema.create(Schema.Type.FLOAT)),
        createPrimitiveUnionFieldSchema("testFloatUnion", Schema.Type.FLOAT),
        createField("testBoolean", Schema.create(Schema.Type.BOOLEAN)),
        createPrimitiveUnionFieldSchema("testBooleanUnion", Schema.Type.BOOLEAN),
        createField("testBytes", Schema.create(Schema.Type.BYTES)),
        createPrimitiveUnionFieldSchema("testBytesUnion", Schema.Type.BYTES));

    GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
    builder.set("testInt", 1);
    builder.set("testIntUnion", 1);
    builder.set("testString", "aaa");
    builder.set("testStringUnion", "aaa");
    builder.set("testJavaString", "aaa");
    builder.set("testJavaStringUnion", "aaa");
    builder.set("testLong", 1L);
    builder.set("testLongUnion", 1L);
    builder.set("testDouble", 1.0);
    builder.set("testDoubleUnion", 1.0);
    builder.set("testFloat", 1.0f);
    builder.set("testFloatUnion", 1.0f);
    builder.set("testBoolean", true);
    builder.set("testBooleanUnion", true);
    builder.set("testBytes", ByteBuffer.wrap(new byte[]{0x01, 0x02}));
    builder.set("testBytesUnion", ByteBuffer.wrap(new byte[]{0x01, 0x02}));
    Record record = builder.build();

    // serialize and deserialize as Row
    Row row = deserialize(recordSchema, recordSchema, serializeAvro(record));
    Row expected = deserializeDummy(recordSchema, recordSchema, serializeAvro(record));

    // Compare with Regular way and Generated Code results
    assertEquals(expected, row);

  }

  @Test
  public void testEnumType() {
    // Prepare a Generic Record
    Schema enumSchema = createEnumSchema("testEnum", new String[]{"A", "B"});
    Schema recordSchema = createRecord("testRecord", createField("testEnum", enumSchema),
        createUnionField("testEnumUnion", enumSchema),
        createArrayFieldSchema("testEnumArray", enumSchema),
        createArrayFieldSchema("testEnumUnionArray", createUnionSchema(enumSchema)));

    GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
    builder.set("testEnum", new GenericData.EnumSymbol(enumSchema, "A"));
    builder.set("testEnumUnion", new GenericData.EnumSymbol(enumSchema, "A"));
    builder.set("testEnumArray", Arrays.asList(new GenericData.EnumSymbol(enumSchema, "A")));
    builder
        .set("testEnumUnionArray", Arrays.asList(new GenericData.EnumSymbol(enumSchema, "A")));
    Record record = builder.build();

    // serialize and deserialize as Row
    Row row = deserialize(recordSchema, recordSchema, serializeAvro(record));
    Row expected = deserializeDummy(recordSchema, recordSchema, serializeAvro(record));

    // Compare with Regular way and Generated Code results
    assertEquals(expected, row);
  }

  @Test
  public void testPermutatedEnumType() {
    // Prepare
    Schema enumSchema = createEnumSchema("testEnum", new String[]{"A", "B", "C", "D", "E"});
    Schema recordSchema = createRecord("testRecord", createField("testEnum", enumSchema),
        createUnionField("testEnumUnion", enumSchema),
        createArrayFieldSchema("testEnumArray", enumSchema),
        createArrayFieldSchema("testEnumUnionArray", createUnionSchema(enumSchema)));

    GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
    builder.set("testEnum", new GenericData.EnumSymbol(enumSchema, "A"));
    builder.set("testEnumUnion", new GenericData.EnumSymbol(enumSchema, "B"));
    builder.set("testEnumArray", Arrays.asList(new GenericData.EnumSymbol(enumSchema, "C")));
    builder
        .set("testEnumUnionArray", Arrays.asList(new GenericData.EnumSymbol(enumSchema, "D")));

    Schema enumSchema1 = createEnumSchema("testEnum", new String[]{"B", "A", "D", "E", "C"});
    Schema recordSchema1 = createRecord("testRecord", createField("testEnum", enumSchema1),
        createUnionField("testEnumUnion", enumSchema1),
        createArrayFieldSchema("testEnumArray", enumSchema1),
        createArrayFieldSchema("testEnumUnionArray", createUnionSchema(enumSchema1)));

    Record record = builder.build();

    // serialize and deserialize as Row
    Row row = deserialize(recordSchema, recordSchema1, serializeAvro(record));
    Row expected = deserializeDummy(recordSchema, recordSchema1, serializeAvro(record));

    // Compare with Regular way and Generated Code results
    assertEquals(expected, row);
  }

  @Test
  public void testNestedRecordType() {
    // given
    Schema subRecordSchema = createRecord("subRecord",
        createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));

    Schema recordSchema = createRecord("test", createUnionField("record", subRecordSchema),
        createField("record1", subRecordSchema),
        createPrimitiveUnionFieldSchema("field", Schema.Type.STRING));

    GenericRecordBuilder subRecordBuilder = new GenericRecordBuilder(subRecordSchema);
    subRecordBuilder.set("subField", "abc");

    GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
    builder.set("record", subRecordBuilder.build());
    builder.set("record1", subRecordBuilder.build());
    builder.set("field", "abc");

    Record record = builder.build();

    // serialize and deserialize as Row
    Row row = deserialize(recordSchema, recordSchema, serializeAvro(record));
    Row expected = deserializeDummy(recordSchema, recordSchema, serializeAvro(record));

    // Compare with Regular way and Generated Code results
    assertEquals(expected, row);
  }

  @Test
  @Disabled
  public void testNestedCollectionsField() {
    // prepare
    Schema subRecordSchema = createRecord("subRecord",
        createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));
    Schema recordSchema = createRecord("test",
        createArrayFieldSchema("recordsArray", subRecordSchema),
        createMapFieldSchema("recordsMap", subRecordSchema),
        createUnionField("recordsArrayUnion",
            Schema.createArray(createUnionSchema(subRecordSchema))),
        createUnionField("recordsMapUnion",
            Schema.createMap(createUnionSchema(subRecordSchema))));

    GenericRecordBuilder subRecordBuilder = new GenericRecordBuilder(subRecordSchema);
    subRecordBuilder.set("subField", "abc");

    GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
    List<GenericData.Record> recordsArray = new ArrayList<>();
    recordsArray.add(subRecordBuilder.build());
    builder.set("recordsArray", recordsArray);
    builder.set("recordsArrayUnion", recordsArray);
    Map<String, GenericData.Record> recordsMap = new HashMap<>();
    recordsMap.put("1", subRecordBuilder.build());
    builder.set("recordsMap", recordsMap);
    builder.set("recordsMapUnion", recordsMap);

    Record record = builder.build();

    // serialize and deserialize as Row
    Row row = deserialize(recordSchema, recordSchema, serializeAvro(record));
    Row expected = deserializeDummy(recordSchema, recordSchema, serializeAvro(record));

    // Compare with Regular way and Generated Code results
    Assertions.assertEquals(expected, row);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFixedType() {
    // given
    Schema fixedSchema = createFixedSchema("testFixed", 2);
    Schema recordSchema = createRecord("testRecord", createField("testFixed", fixedSchema),
        createUnionField("testFixedUnion", fixedSchema),
        createArrayFieldSchema("testFixedArray", fixedSchema),
        createArrayFieldSchema("testFixedUnionArray", createUnionSchema(fixedSchema)));

    GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
    builder.set("testFixed", new GenericData.Fixed(fixedSchema, new byte[]{0x01, 0x02}));
    builder.set("testFixedUnion", new GenericData.Fixed(fixedSchema, new byte[]{0x03, 0x04}));
    builder.set("testFixedArray",
        Arrays.asList(new GenericData.Fixed(fixedSchema, new byte[]{0x05, 0x06})));
    builder.set("testFixedUnionArray",
        Arrays.asList(new GenericData.Fixed(fixedSchema, new byte[]{0x07, 0x08})));

    Record record = builder.build();

    // serialize and deserialize as Row
    Row row = deserialize(recordSchema, recordSchema, serializeAvro(record));
    Row expected = deserializeDummy(recordSchema, recordSchema, serializeAvro(record));

    // Compare with Regular way and Generated Code results
    Assertions.assertEquals(expected, row);
  }

  @Test
  public void testAliasedField() {
    // prepare
    Schema record1Schema = createRecord("test",
        createPrimitiveUnionFieldSchema("testString", Schema.Type.STRING),
        createPrimitiveUnionFieldSchema("testStringUnion", Schema.Type.STRING));
    Schema record2Schema = createRecord(
        "test",
        createPrimitiveUnionFieldSchema("testString", Schema.Type.STRING),
        addAliases(createPrimitiveUnionFieldSchema("testStringUnionAlias", Schema.Type.STRING),
            "testStringUnion"));

    GenericRecordBuilder builder = new GenericRecordBuilder(record1Schema);
    builder.set("testString", "abc");
    builder.set("testStringUnion", "def");

    Record record = builder.build();

    // serialize and deserialize as Row
    Row row = deserialize(record1Schema, record2Schema, serializeAvro(record));
    Row expected = deserializeDummy(record1Schema, record2Schema, serializeAvro(record));

    // Compare with Regular way and Generated Code results
    Assertions.assertEquals(expected, row);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRemovedField() {
    // prepare
    Schema subRecord1Schema = createRecord("subRecord",
        createPrimitiveUnionFieldSchema("testNotRemoved", Schema.Type.STRING),
        createPrimitiveUnionFieldSchema("testRemoved", Schema.Type.STRING),
        createPrimitiveUnionFieldSchema("testNotRemoved2", Schema.Type.STRING));
    Schema record1Schema = createRecord("test",
        createPrimitiveUnionFieldSchema("testNotRemoved", Schema.Type.STRING),
        createPrimitiveUnionFieldSchema("testRemoved", Schema.Type.STRING),
        createPrimitiveUnionFieldSchema("testNotRemoved2", Schema.Type.STRING),
        createUnionField("subRecord", subRecord1Schema));
    //createMapFieldSchema("subRecordMap", subRecord1Schema),
    //createArrayFieldSchema("subRecordArray", subRecord1Schema));

    GenericRecordBuilder subRecordBuilder = new GenericRecordBuilder(subRecord1Schema);
    subRecordBuilder.set("testNotRemoved", "abc");
    subRecordBuilder.set("testRemoved", "def");
    subRecordBuilder.set("testNotRemoved2", "ghi");

    GenericRecordBuilder builder = new GenericRecordBuilder(record1Schema);
    builder.set("testNotRemoved", "abc");
    builder.set("testRemoved", "def");
    builder.set("testNotRemoved2", "ghi");
    builder.set("subRecord", subRecordBuilder.build());
    //builder.set("subRecordArray", Arrays.asList(subRecordBuilder.build()));

    Map<String, GenericRecord> recordsMap = new HashMap<>();
    recordsMap.put("1", subRecordBuilder.build());
    //builder.set("subRecordMap", recordsMap);

    Record record = builder.build();

    Schema subRecord2Schema = createRecord("subRecord",
        createPrimitiveUnionFieldSchema("testNotRemoved", Schema.Type.STRING),
        createPrimitiveUnionFieldSchema("testNotRemoved2", Schema.Type.STRING));
    Schema record2Schema = createRecord("test",
        createPrimitiveUnionFieldSchema("testNotRemoved", Schema.Type.STRING),
        createPrimitiveUnionFieldSchema("testNotRemoved2", Schema.Type.STRING),
        createUnionField("subRecord", subRecord2Schema));
    //createMapFieldSchema("subRecordMap", subRecord2Schema),
    //createArrayFieldSchema("subRecordArray", subRecord2Schema));

    // serialize and deserialize as Row
    Row row = deserialize(record1Schema, record2Schema, serializeAvro(record));
    Row expected = deserializeDummy(record1Schema, record2Schema, serializeAvro(record));

    // Compare with Regular way and Generated Code results
    Assertions.assertEquals(expected, row);
  }

  @Test
  public void testRemovedRecord() {
    // prepare
    Schema subRecord1Schema = createRecord("subRecord",
        createPrimitiveFieldSchema("test1", Schema.Type.STRING),
        createPrimitiveFieldSchema("test2", Schema.Type.STRING));
    Schema subRecord2Schema = createRecord("subRecord2",
        createPrimitiveFieldSchema("test1", Schema.Type.STRING),
        createPrimitiveFieldSchema("test2", Schema.Type.STRING));

    GenericRecordBuilder subRecordBuilder = new GenericRecordBuilder(subRecord1Schema);
    subRecordBuilder.set("test1", "abc");
    subRecordBuilder.set("test2", "def");

    GenericRecordBuilder subRecordBuilder2 = new GenericRecordBuilder(subRecord2Schema);
    subRecordBuilder2.set("test1", "ghi");
    subRecordBuilder2.set("test2", "jkl");

    Schema record1Schema = createRecord("test",
        createField("subRecord1", subRecord1Schema),
        createField("subRecord2", subRecord2Schema),
        createUnionField("subRecord3", subRecord2Schema),
        createField("subRecord4", subRecord1Schema));

    GenericRecordBuilder builder = new GenericRecordBuilder(record1Schema);
    builder.set("subRecord1", subRecordBuilder.build());
    builder.set("subRecord2", subRecordBuilder2.build());
    builder.set("subRecord3", subRecordBuilder2.build());
    builder.set("subRecord4", subRecordBuilder.build());

    Record record = builder.build();

    Schema record2Schema = createRecord("test",
        createField("subRecord1", subRecord1Schema),
        createField("subRecord4", subRecord1Schema));

    // serialize and deserialize as Row
    Row row = deserialize(record1Schema, record2Schema, serializeAvro(record));
    Row expected = deserializeDummy(record1Schema, record2Schema, serializeAvro(record));

    // Compare with Regular way and Generated Code results
    Assertions.assertEquals(expected, row);
  }

  @Test
  public void testRemovedNestedRecord() {
    // prepare
    Schema subSubRecordSchema = createRecord("subSubRecord",
        createPrimitiveFieldSchema("test1", Schema.Type.STRING),
        createPrimitiveFieldSchema("test2", Schema.Type.STRING));
    Schema subRecord1Schema = createRecord("subRecord",
        createPrimitiveFieldSchema("test1", Schema.Type.STRING),
        createField("test2", subSubRecordSchema),
        createUnionField("test3", subSubRecordSchema),
        createPrimitiveFieldSchema("test4", Schema.Type.STRING));

    GenericRecordBuilder subSubRecordBuilder = new GenericRecordBuilder(subSubRecordSchema);
    subSubRecordBuilder.set("test1", "abc");
    subSubRecordBuilder.set("test2", "def");

    GenericRecordBuilder subRecordBuilder = new GenericRecordBuilder(subRecord1Schema);
    subRecordBuilder.set("test1", "abc");
    subRecordBuilder.set("test2", subSubRecordBuilder.build());
    subRecordBuilder.set("test3", subSubRecordBuilder.build());
    subRecordBuilder.set("test4", "def");

    Schema record1Schema = createRecord("test",
        createField("subRecord", subRecord1Schema));

    GenericRecordBuilder builder = new GenericRecordBuilder(record1Schema);
    builder.set("subRecord", subRecordBuilder.build());

    Record record = builder.build();

    Schema subRecord2Schema = createRecord("subRecord",
        createPrimitiveFieldSchema("test1", Schema.Type.STRING),
        createPrimitiveFieldSchema("test4", Schema.Type.STRING));
    Schema record2Schema = createRecord("test",
        createField("subRecord", subRecord2Schema));

    // serialize and deserialize as Row
    Row row = deserialize(record1Schema, record2Schema, serializeAvro(record));
    Row expected = deserializeDummy(record1Schema, record2Schema, serializeAvro(record));

    // Compare with Regular way and Generated Code results
    Assertions.assertEquals(expected, row);
  }

  private Row deserialize(Schema writerSchema, Schema readerSchema, Decoder decoder) {
    RowDeserializer<Row> deserializer = new RowDeserializerCodeGenerator<Row>(writerSchema,
        readerSchema, tempDir, classLoader, null).generateDeserializer();

    try {
      return deserializer.deserialize(decoder);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Row deserializeDummy(Schema writerSchema, Schema readerSchema, Decoder decoder) {
    DatumReader<Row> datumReader = new DummyRowDatumReader<>(writerSchema, readerSchema);
    try {
      return datumReader.read(null, decoder);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
