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
package org.projectnessie.catalog.service.impl;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyList;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import org.apache.avro.file.SeekableFileInput;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergDataFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestEntry;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFileReader;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestListReader;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshot;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.id.NessieIdHasher;
import org.projectnessie.catalog.model.locations.BaseLocation;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.catalog.model.schema.NessieSortDefinition;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.TableFormat;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.storage.backend.CatalogEntitySnapshot;
import org.projectnessie.catalog.storage.backend.CatalogStorage;
import org.projectnessie.catalog.storage.backend.ObjectMismatchException;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
@jakarta.enterprise.context.RequestScoped
public class CatalogServiceImpl implements CatalogService {

  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogServiceImpl.class);

  private final ObjectIO objectIO;
  private final NessieApiV2 nessieApi;
  private final CatalogStorage storage;

  public CatalogServiceImpl() {
    this(null, null, null);
  }

  @Inject
  @jakarta.inject.Inject
  public CatalogServiceImpl(ObjectIO objectIO, NessieApiV2 nessieApi, CatalogStorage storage) {
    this.objectIO = objectIO;
    this.nessieApi = nessieApi;
    this.storage = storage;
  }

  @Override
  public Object tableSnapshot(String ref, ContentKey key, String format, String specVersion)
      throws NessieNotFoundException {
    // TODO remove this log information / move to "trace" / remove sensitive information
    LOGGER.info(
        "tableSnapshot ref:{}  key:{}  format:{}  specVersion:{}", ref, key, format, specVersion);

    ContentResponse content = nessieApi.getContent().refName(ref).getSingle(key);

    NessieId snapshotId = snapshotIdFromContent(content.getContent());

    NessieTableSnapshot snapshot = fetchSnapshot(snapshotId, content);

    if (format == null) {
      // No table format specified, return the NessieTableSnapshot as JSON
      return snapshot;
    }

    switch (TableFormat.valueOf(format.toUpperCase(Locale.ROOT))) {
      case ICEBERG:
        // Return the snapshot as an Iceberg table-metadata using either the spec-version given in
        // the request or the one used when the table-metadata was written.
        // TODO Does requesting a table-metadata using another spec-version make any sense?
        return NessieModelIceberg.nessieTableSnapshotToIceberg(
            snapshot,
            Optional.ofNullable(specVersion).map(Integer::parseInt).map(IcebergSpec::forVersion));
      case DELTA_LAKE:
      default:
        throw new UnsupportedOperationException();
    }
  }

  private NessieTableSnapshot fetchSnapshot(NessieId snapshotId, ContentResponse content) {
    NessieTableSnapshot snapshot;
    CatalogEntitySnapshot catalogSnapshot = storage.loadSnapshot(snapshotId);
    if (catalogSnapshot == null) {
      snapshot = fetchFromObjectStore(snapshotId, content.getContent());
    } else {
      snapshot = fetchFromStorage(catalogSnapshot);
    }
    return snapshot;
  }

  private NessieTableSnapshot fetchFromObjectStore(NessieId snapshotId, Content content) {
    if (content instanceof IcebergTable) {
      try {
        return fetchIcebergTable(snapshotId, (IcebergTable) content);
      } catch (Exception e) {
        // TODO need better error handling here
        throw new RuntimeException(e);
      }
    }
    throw new UnsupportedOperationException("IMPLEMENT ME FOR " + content);
  }

  private NessieTableSnapshot fetchIcebergTable(NessieId snapshotId, IcebergTable content)
      throws IOException, ObjectMismatchException {
    // TODO debug level
    LOGGER.info(
        "Fetching Iceberg table metadata from object store for snapshot ID {} from {}",
        snapshotId,
        content.getMetadataLocation());

    IcebergTableMetadata tableMetadata;
    try {
      tableMetadata =
          IcebergJson.objectMapper()
              .readValue(
                  objectIO.readObject(URI.create(content.getMetadataLocation())),
                  IcebergTableMetadata.class);
    } catch (IOException e) {
      throw new IOException("Failed to read table from " + content.getMetadataLocation(), e);
    }

    NessieId tableId = NessieId.nessieIdFromUUID(UUID.fromString(content.getId()));
    NessieTable table = (NessieTable) storage.loadObject(tableId);

    if (table == null) {
      table =
          NessieTable.builder()
              .id(tableId)
              .createdTimestamp(Instant.ofEpochMilli(tableMetadata.lastUpdatedMs()))
              .tableFormat(TableFormat.ICEBERG)
              .icebergUuid(tableMetadata.tableUuid())
              .icebergLastColumnId(tableMetadata.lastColumnId())
              .icebergLastPartitionId(tableMetadata.lastPartitionId())
              .nessieContentId(content.getId())
              .baseLocation(
                  BaseLocation.baseLocation(
                      NessieId.emptyNessieId(), "dummy", URI.create("dummy://dummy")))
              .build();
    }

    NessieTableSnapshot snapshot =
        NessieModelIceberg.icebergTableSnapshotToNessie(snapshotId, null, table, tableMetadata);

    NessieTableSnapshot shallowSnapshot =
        NessieTableSnapshot.builder()
            .from(snapshot)
            .schemas(emptyList())
            .partitionDefinitions(emptyList())
            .sortDefinitions(emptyList())
            .build();

    Map<NessieId, Object> objectsToStore = new HashMap<>();
    objectsToStore.put(snapshotId, shallowSnapshot);
    objectsToStore.put(snapshot.entity().id(), snapshot.entity());
    snapshot.schemas().forEach(s -> objectsToStore.put(s.schemaId(), s));
    snapshot.partitionDefinitions().forEach(s -> objectsToStore.put(s.partitionDefinitionId(), s));
    snapshot.sortDefinitions().forEach(s -> objectsToStore.put(s.sortDefinitionId(), s));
    CatalogEntitySnapshot catalogSnapshot =
        CatalogEntitySnapshot.catalogEntitySnapshot(
            snapshotId,
            snapshot.entity().id(),
            snapshot.currentSchema(),
            snapshot.schemas().stream().map(NessieSchema::schemaId).collect(Collectors.toList()),
            snapshot.currentPartitionDefinition(),
            snapshot.partitionDefinitions().stream()
                .map(NessiePartitionDefinition::partitionDefinitionId)
                .collect(Collectors.toList()),
            snapshot.currentSortDefinition(),
            snapshot.sortDefinitions().stream()
                .map(NessieSortDefinition::sortDefinitionId)
                .collect(Collectors.toList()));

    LOGGER.info("Storing snapshot from Iceberg table using {} objects", objectsToStore.size());
    storage.createObjects(objectsToStore);
    storage.createSnapshot(catalogSnapshot);

    // Manifest list
    // TODO handling the manifest list needs to separated

    long icebergSnapshotId = tableMetadata.currentSnapshotId();
    tableMetadata.snapshots().stream()
        .filter(s -> s.snapshotId() == icebergSnapshotId)
        .findFirst()
        .ifPresent(this::fetchManifestList);

    return snapshot;
  }

  private void fetchManifestList(IcebergSnapshot icebergSnapshot) {
    icebergSnapshot.manifests();
    if (icebergSnapshot.manifestList() != null && !icebergSnapshot.manifestList().isEmpty()) {
      fetchManifestList(icebergSnapshot.manifestList());
    } else if (!icebergSnapshot.manifests().isEmpty()) {
      fetchManifestsForList(icebergSnapshot.manifests());
    }
  }

  private void fetchManifestList(String manifestListLocation) {
    LOGGER.info("Fetching Iceberg manifest-list from {}", manifestListLocation);

    // TODO Need some tooling to create an Avro `SeekableInput` from an `InputStream`
    try {
      Path tempFile = Files.createTempFile("manifest-list-temp-", ".avro");
      try {
        try (InputStream inputStream = objectIO.readObject(URI.create(manifestListLocation))) {
          Files.copy(inputStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
        }

        try (SeekableFileInput avroInput = new SeekableFileInput(tempFile.toFile());
            IcebergManifestListReader.IcebergManifestListEntryReader entryReader =
                IcebergManifestListReader.builder().build().entryReader(avroInput)) {

          entryReader.spec();
          entryReader.snapshotId();
          entryReader.parentSnapshotId();
          entryReader.sequenceNumber();

          while (entryReader.hasNext()) {
            IcebergManifestFile manifestListEntry = entryReader.next();
            manifestListEntry.addedDataFilesCount();
            manifestListEntry.addedRowsCount();
            manifestListEntry.addedSnapshotId();
            manifestListEntry.manifestPath();
            manifestListEntry.content();
            manifestListEntry.deletedDataFilesCount();
            manifestListEntry.deletedRowsCount();
            manifestListEntry.existingDataFilesCount();
            manifestListEntry.existingRowsCount();
            manifestListEntry.keyMetadata();
            manifestListEntry.manifestLength();
            manifestListEntry.minSequenceNumber();
            manifestListEntry.sequenceNumber();
            manifestListEntry.partitions();
            manifestListEntry.partitionSpecId();
          }
        }

      } finally {
        if (tempFile != null) {
          Files.deleteIfExists(tempFile);
        }
      }
    } catch (Exception e) {
      // TODO some better error handling
      throw new RuntimeException(e);
    }
  }

  private void fetchManifestsForList(List<String> manifests) {
    LOGGER.info(
        "Constructing Iceberg manifest-list from list with {} manifest file locations",
        manifests.size());

    for (String manifest : manifests) {
      // TODO Need some tooling to create an Avro `SeekableInput` from an `InputStream`
      try {
        Path tempFile = Files.createTempFile("manifest-file-temp-", ".avro");
        try {
          try (InputStream inputStream = objectIO.readObject(URI.create(manifest))) {
            Files.copy(inputStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
          }

          try (SeekableFileInput avroInput = new SeekableFileInput(tempFile.toFile());
              IcebergManifestFileReader.IcebergManifestFileEntryReader entryReader =
                  IcebergManifestFileReader.builder().build().entryReader(avroInput)) {
            entryReader.spec();
            entryReader.content();
            entryReader.partitionSpec();
            entryReader.schema();

            long minSeqNum = Long.MAX_VALUE;
            long seqNum = 0L;
            while (entryReader.hasNext()) {
              IcebergManifestEntry entry = entryReader.next();
              Long sequenceNumber = entry.sequenceNumber();
              if (sequenceNumber != null) {
                // TODO is this correct??
                minSeqNum = Math.min(minSeqNum, sequenceNumber);
                seqNum = Math.max(seqNum, sequenceNumber);
              }
              entry.snapshotId();
              entry.fileSequenceNumber();
              entry.status();
              IcebergDataFile dataFile = entry.dataFile();
              dataFile.content();
              dataFile.partition();
              dataFile.specId();
            }

            IcebergManifestFile.Builder manifestFile =
                IcebergManifestFile.builder()
                    .manifestLength(Files.size(tempFile))
                    .manifestPath(manifest)
                    .partitionSpecId(entryReader.partitionSpec().specId());
            if (minSeqNum != Long.MAX_VALUE) {
              manifestFile.minSequenceNumber(minSeqNum).sequenceNumber(seqNum);
            }
            manifestFile.build();
          }

        } finally {
          if (tempFile != null) {
            Files.deleteIfExists(tempFile);
          }
        }
      } catch (Exception e) {
        // TODO some better error handling
        throw new RuntimeException(e);
      }
    }

    throw new UnsupportedOperationException("IMPLEMENT AND CHECK THIS");
  }

  private NessieTableSnapshot fetchFromStorage(CatalogEntitySnapshot catalogSnapshot) {
    // TODO debug level
    LOGGER.info(
        "Fetching table snapshot from database for snapshot ID {} - {} {}",
        catalogSnapshot.snapshotId(),
        catalogSnapshot.schemas().size(),
        catalogSnapshot.partitionDefinitions().size());

    List<NessieId> ids = new ArrayList<>();
    ids.add(catalogSnapshot.entityId());
    ids.add(catalogSnapshot.snapshotId());
    ids.addAll(catalogSnapshot.schemas());
    ids.addAll(catalogSnapshot.partitionDefinitions());
    ids.addAll(catalogSnapshot.sortDefinitions());

    Map<NessieId, Object> loaded = new HashMap<>();
    Set<NessieId> notFound = new HashSet<>();
    storage.loadObjects(ids, loaded::put, notFound::add);

    // TODO some error handling here
    checkState(notFound.isEmpty(), "Ooops - not all parts exist: %s", notFound);

    NessieTableSnapshot.Builder snapshotBuilder =
        NessieTableSnapshot.builder()
            .from((NessieTableSnapshot) loaded.get(catalogSnapshot.snapshotId()))
            .entity((NessieTable) loaded.get(catalogSnapshot.entityId()));
    catalogSnapshot.schemas().stream()
        .map(loaded::get)
        .map(NessieSchema.class::cast)
        .forEach(snapshotBuilder::addSchemas);
    catalogSnapshot.partitionDefinitions().stream()
        .map(loaded::get)
        .map(NessiePartitionDefinition.class::cast)
        .forEach(snapshotBuilder::addPartitionDefinitions);
    catalogSnapshot.sortDefinitions().stream()
        .map(loaded::get)
        .map(NessieSortDefinition.class::cast)
        .forEach(snapshotBuilder::addSortDefinitions);

    NessieTableSnapshot snapshot;
    snapshot = snapshotBuilder.build();
    LOGGER.info(" built: {} {}", snapshot.schemas().size(), snapshot.partitionDefinitions().size());
    return snapshot;
  }

  private NessieId snapshotIdFromContent(Content content) {
    if (content instanceof IcebergTable) {
      IcebergTable icebergTable = (IcebergTable) content;
      return NessieIdHasher.nessieIdHasher()
          .hash(icebergTable.getMetadataLocation())
          .hash(icebergTable.getSnapshotId())
          .generate();
    }
    if (content instanceof IcebergView) {
      throw new UnsupportedOperationException("IMPLEMENT ME");
    }
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }
}
