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

import static com.uyarer.beam.serdes.utils.SerDesUtils.getClassName;
import static com.uyarer.beam.serdes.utils.SerDesUtils.getSchemaKey;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serializer and Deserializer Registry.
 */

public class SerDesRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(SerDesRegistry.class);

  private static volatile SerDesRegistry _INSTANCE;

  private final ConcurrentHashMap<String, RowDeserializer<Row>> rowDeserializers =
      new SerDesConcurrentHashMap<>();
  private final Optional<String> compileClassPath;
  private final Executor executor;
  private URLClassLoader classLoader;
  private File classesDir;
  private Path classesPath;

  /**
   * Registry ser/des generated code which can be used in serializers/deserializers.
   *
   * @param executorService Custom {@link Executor}
   */
  public SerDesRegistry(Executor executorService) {
    this.executor = executorService != null ? executorService : getDefaultExecutor();
    try {
      classesPath = Files.createTempDirectory("generated");
      classesDir = classesPath.toFile();

      classLoader = URLClassLoader.newInstance(new URL[]{classesDir.toURI().toURL()},
          SerDesRegistry.class.getClassLoader());

    } catch (Exception e) {
      LOGGER.warn("Got Error while constructing SerDesRegistry : "
          + Thread.currentThread().getName(), e);
      throw new RuntimeException(e);
    }

    this.compileClassPath = Optional.empty();
  }

  /**
   * Registry ser/des generated code which can be used in serializers/deserializers.
   */
  public SerDesRegistry() {
    this(null);
  }

  /**
   * Based on writerSchema and readerSchema returns Row Deserializer from the registry. If it can
   * not find any {@link RowDeserializer} returns {@link RowDeserializerWithAvroGenericImpl} and
   * starts code generation as async.
   *
   * @param writerSchema the writer's avro schema
   * @param readerSchema the reader's avro schema
   * @return
   */
  public RowDeserializer<Row> getRowDeserializer(Schema writerSchema, Schema readerSchema) {
    String schemaKey = getSchemaKey(writerSchema, readerSchema);
    RowDeserializer<Row> deserializer = rowDeserializers.get(schemaKey);

    //No deserializer on the registry
    if (deserializer == null) {
      LOGGER.info("No deserializer on the registry. Lets Create temporary Dummy Row Reader: "
          + Thread.currentThread().getName());
      //Create temporary Dummy Row Reader
      deserializer = rowDeserializers
          .putIfAbsent(schemaKey,
              new RowDeserializerWithAvroGenericImpl(writerSchema, readerSchema));
      if (deserializer == null) {
        //Lets build a deserializer for this schema combination.
        LOGGER.info("Lets build a deserializer for this schema combination: "
            + Thread.currentThread().getName());
        deserializer = rowDeserializers.get(schemaKey);
        CompletableFuture
            .supplyAsync(() -> {
              LOGGER.warn("Start building a deserializer: " + Thread.currentThread().getName());
              try {
                RowDeserializer buildDeserializer = buildRowDeserializer(writerSchema,
                    readerSchema);
                LOGGER.warn("building a deserializer is done.");
                return buildDeserializer;
              } catch (Throwable e) {
                LOGGER.warn("While building a deserializer got exception: ", e);
                return null;
              }
            }, executor)
            .thenAccept(d -> {
              LOGGER.warn("Replace Dummy Deserializer with Actual one.");
              rowDeserializers.put(schemaKey, d);
            }).join();
      }
    }
    return deserializer;
  }

  private RowDeserializer buildRowDeserializer(Schema writerSchema, Schema readerSchema) {
    try {
      String className = getClassName(writerSchema, readerSchema, "RowDeserializer");
      LOGGER.warn("Lets check is there previously generated java code by someone. className: "
          + className);
      Optional<Path> clazzFile = Files.walk(classesDir.toPath())
          .filter(p -> p.getFileName().startsWith(className + ".class")).findFirst();
      if (clazzFile.isPresent()) {
        LOGGER.warn("Loading className: " + SerDesBase.GENERATED_PACKAGE_NAME_PREFIX + className);
        Class<RowDeserializer<?>> rowDeserializerClass = (Class<RowDeserializer<?>>) classLoader
            .loadClass(
                SerDesBase.GENERATED_PACKAGE_NAME_PREFIX + className);
        return rowDeserializerClass.getConstructor(Schema.class).newInstance(readerSchema);
      } else {
        //We could not find anything. We need to the generate code of deserializer
        LOGGER.warn("We could not find anything. We need to the generate code of deserializer "
            + Thread.currentThread().getName());
        RowDeserializerCodeGenerator generator = new RowDeserializerCodeGenerator(writerSchema,
            readerSchema, classesDir, classLoader, compileClassPath.orElseGet(() -> null));
        RowDeserializer generatedDeserializer = generator.generateDeserializer();
        LOGGER.warn("We generated a deserializer " + Thread.currentThread().getName());
        return generatedDeserializer;
      }
    } catch (Exception e) {
      LOGGER.warn("deserializer class instantiation exception", e);
    }

    LOGGER.warn("If we get any error while generating deserializer or read generated code. "
        + "Lets use slow way. " + Thread.currentThread().getName());
    //If we get any error while generating deserializer or read generated code. Lets use slow way.
    return new RowDeserializerWithAvroGenericImpl(writerSchema, readerSchema);
  }

  /**
   * Get a new {@link SerDesRegistry} object.
   *
   * @return {@link SerDesRegistry}
   */
  public static SerDesRegistry getDefaultInstance() {
    if (_INSTANCE == null) {
      synchronized (SerDesRegistry.class) {
        if (_INSTANCE == null) {
          LOGGER.warn("Creating SerDesRegistry" + Thread.currentThread().getName());
          _INSTANCE = new SerDesRegistry();
        }
      }
    }
    return _INSTANCE;
  }

  private Executor getDefaultExecutor() {
    LOGGER.warn("Creating newFixedThreadPool" + Thread.currentThread().getName());
    return Executors.newFixedThreadPool(2, new ThreadFactory() {
      private final AtomicInteger threadNumber = new AtomicInteger(1);

      @Override
      public Thread newThread(Runnable runnable) {
        Thread thread = Executors.defaultThreadFactory().newThread(runnable);
        thread.setDaemon(true);
        thread.setName("row-serdes-compile-thread-" + threadNumber.getAndIncrement());
        return thread;
      }
    });
  }
}
