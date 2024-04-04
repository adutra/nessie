/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.formats.iceberg.fixtures;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergFixtures.tableMetadataSimple;

import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshot;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;

public class IcebergGenerateFixtures {
  private IcebergGenerateFixtures() {}

  @FunctionalInterface
  public interface ObjectWriter {
    String write(URI name, byte[] data);
  }

  public static ObjectWriter objectWriterForPath(Path path) {
    return (name, data) -> {
      try {
        Path resolved =
            name.isAbsolute() ? Paths.get(name.getPath()) : path.resolve(name.getPath());
        Files.createDirectories(resolved.getParent());
        Files.write(resolved, data);
        return resolved.toString();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  public static String generateSimpleMetadata(ObjectWriter writer, int icebergSpecVersion)
      throws Exception {
    IcebergTableMetadata simpleTableMetadata =
        tableMetadataSimple().formatVersion(icebergSpecVersion).build();
    return writer.write(
        URI.create("table-metadata-simple-no-manifest/table-metadata-simple-no-manifest.json"),
        IcebergJson.objectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .writeValueAsString(simpleTableMetadata)
            .getBytes(UTF_8));
  }

  public static String generateMetadataWithManifestList(String basePath, ObjectWriter writer)
      throws Exception {
    IcebergSchemaGenerator schemaGenerator =
        IcebergSchemaGenerator.spec().numColumns(10).numPartitionColumns(2).generate();

    Function<String, OutputStream> outputFunction =
        file ->
            new ByteArrayOutputStream() {
              @Override
              public void close() throws IOException {
                super.close();
                writer.write(URI.create(file), toByteArray());
              }
            };
    UUID commitId = randomUUID();
    long snapshotId = 1;
    long sequenceNumber = 1;
    IcebergManifestFileGenerator manifestFileGenerator =
        IcebergManifestFileGenerator.builder()
            .addDataFiles(3)
            .basePath(basePath)
            .output(outputFunction)
            .addedSnapshotId(snapshotId)
            .fileSequenceNumber(sequenceNumber)
            .sequenceNumber(sequenceNumber)
            .minSequenceNumber(sequenceNumber)
            .generator(schemaGenerator)
            .build();
    String manifestList =
        IcebergManifestListGenerator.builder()
            .manifestFileCount(10)
            .sequenceNumber(1)
            .commitId(commitId)
            .basePath(basePath)
            .output(outputFunction)
            .generator(schemaGenerator)
            .build()
            .generate(manifestFileGenerator.createSupplier(commitId));
    IcebergSnapshot snapshotWithManifestList =
        IcebergSnapshot.builder()
            .snapshotId(snapshotId)
            .timestampMs(System.currentTimeMillis())
            .schemaId(schemaGenerator.getIcebergSchema().schemaId())
            .sequenceNumber(sequenceNumber)
            .manifestList(manifestList)
            .putSummary("operation", "ADD")
            .build();
    IcebergTableMetadata icebergMetadataWithManifestList =
        IcebergTableMetadata.builder()
            .from(tableMetadataSimple().formatVersion(2).build())
            .schemas(singletonList(schemaGenerator.getIcebergSchema()))
            .partitionSpecs(singletonList(schemaGenerator.getIcebergPartitionSpec()))
            .currentSnapshotId(snapshotId)
            .defaultSpecId(schemaGenerator.getIcebergPartitionSpec().specId())
            .snapshots(singletonList(snapshotWithManifestList))
            .build();
    return writer.write(
        URI.create("table-metadata-with-manifest-list/table-metadata-with-manifest-list.json"),
        IcebergJson.objectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .writeValueAsString(icebergMetadataWithManifestList)
            .getBytes(UTF_8));
  }

  public static String generateMetadataWithManifests(String basePath, ObjectWriter writer)
      throws Exception {
    IcebergSchemaGenerator schemaGenerator =
        IcebergSchemaGenerator.spec().numColumns(10).numPartitionColumns(2).generate();
    Function<String, OutputStream> outputFunction =
        file -> {
          try {
            return Files.newOutputStream(Paths.get(file));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        };
    UUID commitId = randomUUID();
    long snapshotId = 1;
    long sequenceNumber = 1;
    List<String> manifestFiles = new ArrayList<>();
    IcebergManifestFileGenerator manifestFileGenerator =
        IcebergManifestFileGenerator.builder()
            .addDataFiles(3)
            .basePath(basePath)
            .output(
                f -> {
                  manifestFiles.add(f);
                  return outputFunction.apply(f);
                })
            .addedSnapshotId(snapshotId)
            .fileSequenceNumber(sequenceNumber)
            .sequenceNumber(sequenceNumber)
            .minSequenceNumber(sequenceNumber)
            .generator(schemaGenerator)
            .build();

    IcebergManifestListGenerator.builder()
        .manifestFileCount(10)
        .sequenceNumber(1)
        .commitId(commitId)
        .basePath(basePath)
        .output(outputFunction)
        .generator(schemaGenerator)
        .build()
        .generate(manifestFileGenerator.createSupplier(commitId));

    IcebergSnapshot snapshotWithManifests =
        IcebergSnapshot.builder()
            .snapshotId(snapshotId)
            .timestampMs(System.currentTimeMillis())
            .schemaId(schemaGenerator.getIcebergSchema().schemaId())
            .sequenceNumber(sequenceNumber)
            .manifests(manifestFiles)
            .putSummary("operation", "append")
            .build();
    IcebergTableMetadata icebergMetadataWithManifests =
        IcebergTableMetadata.builder()
            .from(tableMetadataSimple().formatVersion(1).build())
            .schemas(singletonList(schemaGenerator.getIcebergSchema()))
            .partitionSpecs(singletonList(schemaGenerator.getIcebergPartitionSpec()))
            .currentSnapshotId(snapshotId)
            .defaultSpecId(schemaGenerator.getIcebergPartitionSpec().specId())
            .snapshots(singletonList(snapshotWithManifests))
            .build();
    return writer.write(
        URI.create("table-metadata-with-manifests/table-metadata-with-manifests.json"),
        IcebergJson.objectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .writeValueAsString(icebergMetadataWithManifests)
            .getBytes(UTF_8));
  }
}
