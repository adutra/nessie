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
import java.io.IOException;
import java.io.OutputStream;
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

  public static String generateSimpleMetadata(Path targetDir) throws Exception {
    targetDir = targetDir.resolve("table-metadata-simple-no-manifest");
    Files.createDirectories(targetDir);

    Path metadataSimpleFile = targetDir.resolve("table-metadata-simple-no-manifest.json");
    IcebergTableMetadata simpleTableMetadata = tableMetadataSimple().formatVersion(2).build();
    Files.write(
        metadataSimpleFile,
        IcebergJson.objectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .writeValueAsString(simpleTableMetadata)
            .getBytes(UTF_8));
    return metadataSimpleFile.toUri().toString();
  }

  public static String generateMetadataWithManifestList(Path targetDir) throws Exception {
    targetDir = targetDir.resolve("table-metadata-with-manifest-list");
    Files.createDirectories(targetDir);

    Path metadataWithManifestList = targetDir.resolve("table-metadata-with-manifest-list.json");
    IcebergSchemaGenerator schemaGenerator =
        IcebergSchemaGenerator.spec().numColumns(10).numPartitionColumns(2).generate();
    String basePath = targetDir.toString() + '/';
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
    Files.write(
        metadataWithManifestList,
        IcebergJson.objectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .writeValueAsString(icebergMetadataWithManifestList)
            .getBytes(UTF_8));
    return metadataWithManifestList.toUri().toString();
  }

  public static String generateMetadataWithManifests(Path targetDir) throws Exception {
    targetDir = targetDir.resolve("table-metadata-with-manifests");
    Files.createDirectories(targetDir);

    Path metadataWithManifests = targetDir.resolve("table-metadata-with-manifests.json");
    IcebergSchemaGenerator schemaGenerator =
        IcebergSchemaGenerator.spec().numColumns(10).numPartitionColumns(2).generate();
    String basePath = targetDir.toString() + '/';
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
    Files.write(
        metadataWithManifests,
        IcebergJson.objectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .writeValueAsString(icebergMetadataWithManifests)
            .getBytes(UTF_8));

    return metadataWithManifests.toUri().toString();
  }
}
