/*
 * Copyright (C) 2023 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.catalog.formats.iceberg.meta;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.avro.file.SeekableByteArrayInput;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.projectnessie.catalog.formats.iceberg.fixtures.IcebergManifestFileGenerator;
import org.projectnessie.catalog.formats.iceberg.fixtures.IcebergSchemaGenerator;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFileReader;

@Warmup(iterations = 2, time = 2000, timeUnit = MILLISECONDS)
@Measurement(iterations = 3, time = 1000, timeUnit = MILLISECONDS)
@Fork(1)
@Threads(
    1) // The 'fileSize' aux-counter only meaningful when running the benchmark with ONE THREAD!
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
public class IcebergManifestFileSerializationBench {
  @State(Scope.Benchmark)
  public static class BenchmarkParam {
    /** Format: number of long columns, number of string columns. */
    @Param({"100:0", "0:100"}) // "1000:0", "0:1000"
    public String columns;

    @Param({"gzip"}) // , "snappy", "zstd", "uncompressed"})
    public String avroCompression;

    @Param({"100", "1000"}) // , "snappy", "zstd", "uncompressed"})
    public int dataFiles;

    IcebergSchemaGenerator schemaGenerator;
    String basePath;

    private byte[] serializedManifestFile;

    @Setup
    public void init() throws Exception {
      String[] tuple = columns.split(":");
      int numLong = Integer.parseInt(tuple[0].trim());
      int numString = Integer.parseInt(tuple[1].trim());

      schemaGenerator =
          IcebergSchemaGenerator.spec()
              .numColumns(numLong)
              .numTextColumns(numString, 5, 16)
              .numPartitionColumns(1)
              .numTextPartitionColumns(0, 5, 10)
              .generate();

      basePath = "file:///foo/" + "1234567890".repeat(100) + "/baz/";

      ByteArrayOutputStream collect = new ByteArrayOutputStream();
      doGenerate(this, path -> collect);
      this.serializedManifestFile = collect.toByteArray();
    }
  }

  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  public static class SizeResult {
    // This value is only meaningful when running the benchmark with ONE THREAD
    public long fileSize;
  }

  @Benchmark
  public IcebergManifestFile serializeManifestFile(BenchmarkParam param, SizeResult sizeResult) {
    Function<String, OutputStream> output =
        path ->
            countingNullOutput(
                path,
                (p, size) -> {
                  if (sizeResult.fileSize == 0L) {
                    sizeResult.fileSize = size;
                  }
                });

    return doGenerate(param, output);
  }

  @Benchmark
  public void deserializeManifestFile(BenchmarkParam param, Blackhole bh) throws Exception {
    try (IcebergManifestFileReader.IcebergManifestFileEntryReader entryReader =
        IcebergManifestFileReader.builder()
            .build()
            .entryReader(new SeekableByteArrayInput(param.serializedManifestFile))) {
      while (entryReader.hasNext()) {
        bh.consume(entryReader.next());
      }
    }
  }

  private static IcebergManifestFile doGenerate(
      BenchmarkParam param, Function<String, OutputStream> output) {
    IcebergManifestFileGenerator manifestFileGenerator =
        IcebergManifestFileGenerator.builder()
            .generator(param.schemaGenerator)
            .addDataFiles(1)
            .avroCompression(param.avroCompression)
            .basePath(param.basePath)
            .output(output)
            .addDataFiles(param.dataFiles)
            .build();

    return manifestFileGenerator.createSupplier(UUID.randomUUID()).get();
  }

  static OutputStream countingNullOutput(String path, BiConsumer<String, Long> fileSize) {
    return new OutputStream() {
      long size = 0;

      @Override
      public void write(int b) {
        size++;
      }

      @Override
      public void write(byte[] b, int off, int len) {
        size += len;
      }

      @Override
      public void close() {
        fileSize.accept(path, size);
      }
    };
  }
}
