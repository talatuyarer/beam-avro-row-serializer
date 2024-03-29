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

import static com.uyarer.beam.serdes.utils.SerDesUtils.generateSourcePathFromPackageName;

import com.uyarer.beam.serdes.exceptions.RowSerdesGeneratorException;
import com.uyarer.beam.serdes.utils.SerDesUtils;
import com.sun.codemodel.JCodeModel;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SerDesBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(SerDesBase.class);
  public static final String GENERATED_PACKAGE_NAME_PREFIX =
      "com.uyarer.beam.serdes.generated.";

  private final ConcurrentMap<String, AtomicInteger> counterPerName =
      new SerDesConcurrentHashMap<>();
  private final String generatedSourcesPath;
  protected final String generatedPackageName;
  protected final JCodeModel codeModel = new JCodeModel();
  protected final SchemaCodeGeneratorHelper schemaCodeGeneratorHelper;
  protected final File destination;
  protected final ClassLoader classLoader;
  protected final String compileClassPath;

  /**
   * Shared functions can be used by both serialization and deserialization code.
   *
   * @param description      Definition of action such as deserializer or serializer etc.
   * @param destination      Generated code path.
   * @param classLoader      Class Loader
   * @param compileClassPath Compiled class location.
   */
  public SerDesBase(String description, File destination, ClassLoader classLoader,
      String compileClassPath) {
    this.schemaCodeGeneratorHelper = new SchemaCodeGeneratorHelper(codeModel);
    this.destination = destination;
    this.classLoader = classLoader;
    this.compileClassPath = (null == compileClassPath ? "" : compileClassPath);
    this.generatedPackageName = GENERATED_PACKAGE_NAME_PREFIX + description;
    this.generatedSourcesPath = generateSourcePathFromPackageName(generatedPackageName);
  }

  /**
   * A function to generate unique names, such as those of variables and functions, within the scope
   * of the this class instance (i.e. per serializer of a given schema or deserializer of a given
   * schema pair).
   *
   * @param prefix String to serve as a prefix for the unique name
   * @return a unique prefix composed of the prefix appended by a unique number
   */
  protected String getUniqueName(String prefix) {
    String uncapitalizedPrefix = StringUtils.uncapitalize(prefix)
        .replaceAll("[\\W+]", "_");
    return uncapitalizedPrefix + nextUniqueInt(uncapitalizedPrefix);
  }

  private int nextUniqueInt(String name) {
    return counterPerName.computeIfAbsent(name, k -> new AtomicInteger(0)).getAndIncrement();
  }

  @SuppressWarnings("unchecked")
  protected Class compileClass(final String className,
      Set<String> knownUsedFullyQualifiedClassNameSet)
      throws IOException, ClassNotFoundException {
    codeModel.build(destination);

    String filePath = destination.getAbsolutePath() + generatedSourcesPath + className + ".java";
    LOGGER.info("Generated Code Destination: " + filePath);

    //Main compiler = new Main();
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    if (compiler == null) {
      throw new RowSerdesGeneratorException("No compiler generated.");
    }
    String compileClassPathForCurrentFile = SerDesUtils
        .inferCompileDependencies(compileClassPath, filePath, knownUsedFullyQualifiedClassNameSet);
    int compileResult;
    try {
      LOGGER.info("Starting compilation for the generated source file: {} ", filePath);
      LOGGER.debug("The inferred compile class path for file: {} : {}", filePath,
          compileClassPathForCurrentFile);
      String[] args = new String[]{
          "-cp", compileClassPathForCurrentFile,
          filePath,
          "-XDuseUnsharedTable"
      };
      //compileResult = compiler.compile(args);
      compileResult = compiler.run(null, null, null, args);
    } catch (Exception e) {
      throw new RowSerdesGeneratorException(
          "Unable to compile:" + className + " from source file: " + filePath, e);
    }

    if (compileResult != 0) {
      throw new RowSerdesGeneratorException(
          "Unable to compile:" + className + " from source file: " + filePath);
    } else {
      LOGGER.info("Successfully compiled class {} defined at source file: {}", className, filePath);
    }

    return classLoader.loadClass(generatedPackageName + "." + className);
  }

}
