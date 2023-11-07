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
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import org.projectnessie.catalog.formats.iceberg.fixtures.IcebergManifestListGenerator;
import org.projectnessie.catalog.formats.iceberg.fixtures.IcebergSchemaGenerator;
import org.projectnessie.catalog.formats.iceberg.fixtures.ImmutableIcebergManifestListGenerator;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestListReader;

@Warmup(iterations = 2, time = 2000, timeUnit = MILLISECONDS)
@Measurement(iterations = 3, time = 1000, timeUnit = MILLISECONDS)
@Fork(1)
@Threads(
    1) // The 'fileSize' aux-counter only meaningful when running the benchmark with ONE THREAD!
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
public class IcebergManifestListSerializationBench {
  @State(Scope.Benchmark)
  public static class BenchmarkParam {
    /** Format: number of long partitions, number of string partitions */
    @Param({"4:0", "0:4"})
    public String columns;

    @Param({"gzip"}) // , "snappy", "zstd", "uncompressed"})
    public String avroCompression;

    @Param({"1", "10", "100", "1000", "10000"})
    public int manifestFiles;

    IcebergSchemaGenerator schemaGenerator;
    String basePath;
    List<IcebergManifestFile> files;

    private byte[] serializedManifestList;

    @Setup
    public void init() throws Exception {
      String[] tuple = columns.split(":");
      int numLong = Integer.parseInt(tuple[0].trim());
      int numString = Integer.parseInt(tuple[1].trim());

      schemaGenerator =
          IcebergSchemaGenerator.spec()
              .numColumns(1)
              .numTextColumns(1, 5, 16)
              .numPartitionColumns(numLong)
              .numTextPartitionColumns(numString, 5, 10)
              .generate();

      basePath = "file:///foo/" + "1234567890".repeat(100) + "/baz/";

      IcebergManifestFileGenerator manifestFileGenerator =
          IcebergManifestFileGenerator.builder()
              .generator(schemaGenerator)
              .addDataFiles(1)
              .avroCompression(avroCompression)
              .basePath(basePath)
              .output(path -> OutputStream.nullOutputStream())
              .build();

      Supplier<IcebergManifestFile> supplier =
          manifestFileGenerator.createSupplier(UUID.randomUUID());

      files =
          IntStream.range(0, manifestFiles)
              .mapToObj(x -> supplier.get())
              .collect(Collectors.toList());

      ByteArrayOutputStream collect = new ByteArrayOutputStream();
      doGenerate(this, path -> collect);
      this.serializedManifestList = collect.toByteArray();
    }
  }

  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  public static class SizeResult {
    // This value is only meaningful when running the benchmark with ONE THREAD
    public long fileSize;
  }

  @Benchmark
  public void serializeManifestList(BenchmarkParam param, SizeResult sizeResult) throws Exception {
    Function<String, OutputStream> output =
        path ->
            countingNullOutput(
                path,
                (p, size) -> {
                  if (sizeResult.fileSize == 0L) {
                    sizeResult.fileSize = size;
                  }
                });

    doGenerate(param, output);
  }

  @Benchmark
  public void deserializeManifestList(BenchmarkParam param, Blackhole bh) throws Exception {
    try (IcebergManifestListReader.IcebergManifestListEntryReader entryReader =
        IcebergManifestListReader.builder()
            .build()
            .entryReader(new SeekableByteArrayInput(param.serializedManifestList))) {
      while (entryReader.hasNext()) {
        bh.consume(entryReader.next());
      }
    }
  }

  private static void doGenerate(BenchmarkParam param, Function<String, OutputStream> output)
      throws Exception {
    ImmutableIcebergManifestListGenerator listGen =
        IcebergManifestListGenerator.builder()
            .basePath(param.basePath)
            .output(output)
            .generator(param.schemaGenerator)
            .avroCompression(param.avroCompression)
            .snapshotId(42)
            .commitId(UUID.randomUUID())
            .sequenceNumber(43)
            .attempt(1)
            .manifestFileCount(param.manifestFiles)
            .build();

    Iterator<IcebergManifestFile> iter = param.files.iterator();

    listGen.generate(iter::next);
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
