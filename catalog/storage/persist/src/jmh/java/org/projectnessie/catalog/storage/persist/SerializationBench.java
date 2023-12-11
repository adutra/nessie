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
package org.projectnessie.catalog.storage.persist;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import com.fasterxml.jackson.dataformat.ion.IonFactory;
import com.fasterxml.jackson.dataformat.ion.IonObjectMapper;
import com.fasterxml.jackson.dataformat.smile.databind.SmileMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Locale;
import java.util.StringTokenizer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;
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
import org.projectnessie.catalog.formats.iceberg.fixtures.IcebergSchemaGenerator;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

// NOTE: Use `java -Djmh.executor=VIRTUAL -jar <jmhjar>` to exercise virtual threads (to verify that
// compression libs work fine).
@Warmup(iterations = 3, time = 2000, timeUnit = MILLISECONDS)
@Measurement(iterations = 3, time = 1000, timeUnit = MILLISECONDS)
@Fork(1)
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
public class SerializationBench {

  // gzip:
  // 78 1 74 bd e9 6b 73 d1 a3 d7
  // snappy:
  // 82 53 4e 41 50 50 59 0 0 0
  // zstd:
  // 28 b5 2f fd <-- https://github.com/facebook/zstd/blob/dev/doc/zstd_compression_format.md

  @State(Scope.Benchmark)
  public static class BenchmarkParam {
    /** Format: number of long partitions, number of string partitions. */
    @Param({"250:250"})
    public String columns;

    @Param({"json", "cbor", "smile", "ion"})
    public String format;

    @Param({"nessieSchema", "nessiePartitionDefinition", "icebergSchema", "icebergPartitionSpec"})
    public String type;

    @Param({"uncompressed"}) // "gzip", "snappy", "zstd"})
    public String compression;

    IcebergSchemaGenerator schemaGenerator;

    ObjectMapper mapper;

    Class<?> typeClass;
    Object object;
    byte[] serialized;

    Function<byte[], byte[]> compress;
    Function<byte[], byte[]> uncompress;

    Supplier<Deflater> deflaterSupplier;
    Supplier<Inflater> inflaterSupplier;

    @Setup
    public void init() throws Exception {
      String[] tuple = columns.split(":");
      int numLong = Integer.parseInt(tuple[0].trim());
      int numString = Integer.parseInt(tuple[1].trim());

      schemaGenerator =
          IcebergSchemaGenerator.spec()
              .numColumns(numLong)
              .numTextColumns(numString, 5, 16)
              .numPartitionColumns(numLong)
              .numTextPartitionColumns(numString, 5, 10)
              .generate();

      switch (format) {
        case "json":
          mapper = new ObjectMapper();
          break;
        case "cbor":
          mapper = new CBORMapper();
          break;
        case "smile":
          mapper = new SmileMapper();
          break;
        case "ion":
          IonFactory ionFactory = new IonFactory();
          ionFactory.setCreateBinaryWriters(true);
          mapper = new IonObjectMapper(ionFactory);
          break;
        default:
          throw new IllegalArgumentException("Unknown serialization format " + format);
      }
      mapper.registerModule(new GuavaModule());

      switch (compression) {
        case "uncompressed":
          compress = Function.identity();
          uncompress = Function.identity();
          break;
        case "snappy":
          compress = SerializationBench::compressSnappy;
          uncompress = SerializationBench::uncompressSnappy;
          break;
        case "zstd":
          compress = SerializationBench::compressZstd;
          uncompress = SerializationBench::uncompressZstd;
          break;
        default:
          if (compression.startsWith("gzip")) {
            StringTokenizer st = new StringTokenizer(compression, "-");
            int level = Deflater.DEFAULT_COMPRESSION;
            int strategy = Deflater.DEFAULT_STRATEGY;
            if (st.hasMoreTokens()) {
              st.nextToken(); // skip "gzip"
            }
            if (st.hasMoreTokens()) {
              try {
                level = Integer.parseInt(st.nextToken());
                checkArgument(level >= 0 && level <= 9, "Invalid gzip compression");
              } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid gzip compression");
              }
            }
            if (st.hasMoreTokens()) {
              switch (st.nextToken().toLowerCase(Locale.ROOT)) {
                case "default":
                  strategy = Deflater.DEFAULT_STRATEGY;
                  break;
                case "filtered":
                  strategy = Deflater.FILTERED;
                  break;
                case "huffman":
                  strategy = Deflater.HUFFMAN_ONLY;
                  break;
                default:
                  throw new IllegalArgumentException("Unknown gzip strategy");
              }
            }
            int effLevel = level;
            int effStrategy = strategy;
            deflaterSupplier =
                () -> {
                  Deflater def = new Deflater(effLevel);
                  def.setStrategy(effStrategy);
                  return def;
                };
            inflaterSupplier = Inflater::new;
            compress = this::compressGzip;
            uncompress = this::uncompressGzip;
          } else {
            throw new IllegalArgumentException("Unknown compression type " + compression);
          }
          break;
      }

      switch (type) {
        case "nessieSchema":
          object = schemaGenerator.getNessieSchema();
          typeClass = NessieSchema.class;
          break;
        case "nessiePartitionDefinition":
          object = schemaGenerator.getNessiePartitionDefinition();
          typeClass = NessiePartitionDefinition.class;
          break;
        case "icebergSchema":
          object = schemaGenerator.getIcebergSchema();
          typeClass = IcebergSchema.class;
          break;
        case "icebergPartitionSpec":
          object = schemaGenerator.getIcebergPartitionSpec();
          typeClass = IcebergPartitionSpec.class;
          break;
        default:
          throw new IllegalArgumentException("Unknown object type " + type);
      }

      serialized = doSerialize(this);
    }

    byte[] compressGzip(byte[] bytes) {
      ByteArrayOutputStream out = new ByteArrayOutputStream(bytes.length);
      try (OutputStream def = new DeflaterOutputStream(out, deflaterSupplier.get())) {
        def.write(bytes);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return out.toByteArray();
    }

    byte[] uncompressGzip(byte[] bytes) {
      ByteArrayOutputStream out = new ByteArrayOutputStream(bytes.length * 5);
      try (OutputStream def = new InflaterOutputStream(out, inflaterSupplier.get())) {
        def.write(bytes);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return out.toByteArray();
    }
  }

  private static byte[] compressSnappy(byte[] bytes) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(bytes.length);
    try (OutputStream def = new SnappyOutputStream(out)) {
      def.write(bytes);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return out.toByteArray();
  }

  private static byte[] uncompressSnappy(byte[] bytes) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(bytes.length * 5);
    try (InputStream input = new SnappyInputStream(new ByteArrayInputStream(bytes))) {
      input.transferTo(out);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return out.toByteArray();
  }

  private static byte[] compressZstd(byte[] bytes) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(bytes.length);
    try (OutputStream def = new ZstdOutputStream(out)) {
      def.write(bytes);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return out.toByteArray();
  }

  private static byte[] uncompressZstd(byte[] bytes) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(bytes.length * 5);
    try (InputStream input = new ZstdInputStream(new ByteArrayInputStream(bytes))) {
      input.transferTo(out);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return out.toByteArray();
  }

  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  public static class SizeResult {
    // This value is only meaningful when running the benchmark with ONE THREAD
    public long serializedSize;
  }

  @Benchmark
  public byte[] serialize(BenchmarkParam param, SizeResult sizeResult) throws Exception {
    byte[] bytes = doSerialize(param);
    if (sizeResult.serializedSize == 0L) {
      sizeResult.serializedSize = bytes.length;
    }
    return bytes;
  }

  private static byte[] doSerialize(BenchmarkParam param) throws JsonProcessingException {
    byte[] uncompressed = param.mapper.writeValueAsBytes(param.object);
    return param.compress.apply(uncompressed);
  }

  @Benchmark
  public Object deserialize(BenchmarkParam param) throws Exception {
    return doDeserialize(param);
  }

  private static Object doDeserialize(BenchmarkParam param) throws IOException {
    byte[] uncompressed = param.uncompress.apply(param.serialized);
    return param.mapper.readValue(uncompressed, param.typeClass);
  }
}
