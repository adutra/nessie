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

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergDataFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestContent;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestEntry;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFileReader;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestListReader;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestListWriter;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionFieldSummary;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshot;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.id.NessieIdHasher;
import org.projectnessie.catalog.model.locations.BaseLocation;
import org.projectnessie.catalog.model.manifest.NessieFieldSummary;
import org.projectnessie.catalog.model.manifest.NessieFileContentType;
import org.projectnessie.catalog.model.manifest.NessieListManifestEntry;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.catalog.model.schema.NessieSortDefinition;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.TableFormat;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SnapshotFormat;
import org.projectnessie.catalog.service.api.SnapshotResponse;
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
public class CatalogServiceImpl implements CatalogService {

  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogServiceImpl.class);

  private final ObjectIO objectIO;
  private final NessieApiV2 nessieApi;
  private final CatalogStorage storage;

  public CatalogServiceImpl() {
    this(null, null, null);
  }

  @Inject
  public CatalogServiceImpl(ObjectIO objectIO, NessieApiV2 nessieApi, CatalogStorage storage) {
    this.objectIO = objectIO;
    this.nessieApi = nessieApi;
    this.storage = storage;
  }

  @Override
  public SnapshotResponse retrieveTableSnapshot(
      String ref, ContentKey key, SnapshotFormat format, OptionalInt specVersion)
      throws NessieNotFoundException {
    // TODO remove this log information / move to "trace" / remove sensitive information
    LOGGER.info("retrieveTableSnapshot ref:{}  key:{}", ref, key);

    ContentResponse content = nessieApi.getContent().refName(ref).getSingle(key);

    NessieId snapshotId = snapshotIdFromContent(content.getContent());

    // TODO only retrieve objects that are required. For example:
    //  Manifest-list-entries are not required when returning an Iceberg table-metadata JSON.
    //  Other parts are not required when returning only an Iceberg manifest-list.
    NessieTableSnapshot snapshot = retrieveTableSnapshot(snapshotId, content);
    Object result;
    String suffix;
    String fileNameBase = key.toPathString() + '-' + snapshot.snapshotId().idAsString();

    switch (format) {
      case NESSIE_SNAPSHOT:
        suffix = "-nessie";
        result = snapshot;
        break;
      case ICEBERG_TABLE_METADATA:
        // Return the snapshot as an Iceberg table-metadata using either the spec-version given in
        // the request or the one used when the table-metadata was written.
        // TODO Does requesting a table-metadata using another spec-version make any sense?
        // TODO Response should respect the JsonView / spec-version
        // TODO Add a check that the original table format was Iceberg (not Delta)
        Optional<IcebergSpec> icebergSpec =
            specVersion.isPresent()
                ? Optional.of(IcebergSpec.forVersion(specVersion.getAsInt()))
                : Optional.empty();
        IcebergTableMetadata tableMetadata =
            NessieModelIceberg.nessieTableSnapshotToIceberg(snapshot, icebergSpec);
        result = tableMetadata;
        suffix = "-iceberg-table-meta-v" + tableMetadata.formatVersion();
        break;
      case ICEBERG_MANIFEST_LIST:
        icebergSpec =
            specVersion.isPresent()
                ? Optional.of(IcebergSpec.forVersion(specVersion.getAsInt()))
                : Optional.empty();
        return produceIcebergManifestList(fileNameBase, snapshot, icebergSpec);
      default:
        throw new IllegalArgumentException("Unknown format " + format);
    }

    String fileName = fileNameBase + suffix + ".json";

    return SnapshotResponse.forEntity(result, fileName);
  }

  private SnapshotResponse produceIcebergManifestList(
      String fileNameBase, NessieTableSnapshot snapshot, Optional<IcebergSpec> icebergSpec) {
    return new SnapshotResponse() {
      @Override
      public Optional<Object> entityObject() {
        return Optional.empty();
      }

      @Override
      public String fileName() {
        return fileNameBase + "-iceberg-manifest-list.avro";
      }

      @Override
      public void produce(OutputStream outputStream) throws IOException {
        NessieSchema schema =
            snapshot.schemas().stream()
                .filter(s -> s.schemaId().equals(snapshot.currentSchema()))
                .findFirst()
                .orElseThrow();
        NessiePartitionDefinition partitionDefinition =
            snapshot.partitionDefinitions().stream()
                .filter(
                    p -> p.partitionDefinitionId().equals(snapshot.currentPartitionDefinition()))
                .findFirst()
                .orElseThrow();

        try (IcebergManifestListWriter.IcebergManifestListEntryWriter writer =
            IcebergManifestListWriter.builder()
                .spec(icebergSpec.orElse(IcebergSpec.forVersion(snapshot.icebergFormatVersion())))
                .schema(NessieModelIceberg.nessieSchemaToIcebergSchema(schema))
                .partitionSpec(
                    NessieModelIceberg.nessiePartitionDefinitionToIceberg(partitionDefinition))
                // .parentSnapshotId()  - TODO ??
                .snapshotId(snapshot.icebergSnapshotId())
                .tableProperties(snapshot.properties())
                //                .sequenceNumber(snapshot.icebergSnapshotSequenceNumber())
                .build()
                .entryWriter(outputStream)) {

          for (NessieListManifestEntry manifest : snapshot.manifests()) {
            IcebergManifestContent content;
            switch (manifest.content()) {
              case ICEBERG_DATA_FILE:
                content = IcebergManifestContent.DATA;
                break;
              case ICEBERG_DELETE_FILE:
                content = IcebergManifestContent.DELETES;
                break;
              default:
                throw new IllegalArgumentException(manifest.content().name());
            }
            List<IcebergPartitionFieldSummary> partitions =
                manifest.partitions().stream()
                    .map(
                        p ->
                            IcebergPartitionFieldSummary.builder()
                                .containsNan(p.containsNan())
                                .containsNull(p.containsNull())
                                .lowerBound(toByteBuffer(p.lowerBound()))
                                .upperBound(toByteBuffer(p.upperBound()))
                                .build())
                    .collect(Collectors.toList());
            writer.append(
                IcebergManifestFile.builder()
                    .content(content)
                    //
                    .manifestPath(manifest.manifestPath())
                    .manifestLength(manifest.manifestLength())
                    //
                    .addedSnapshotId(manifest.addedSnapshotId())
                    .sequenceNumber(manifest.sequenceNumber())
                    .minSequenceNumber(manifest.minSequenceNumber())
                    .partitionSpecId(manifest.partitionSpecId())
                    .partitions(partitions)
                    //
                    .addedDataFilesCount(manifest.addedDataFilesCount())
                    .addedRowsCount(manifest.addedRowsCount())
                    .deletedDataFilesCount(manifest.deletedDataFilesCount())
                    .deletedRowsCount(manifest.deletedRowsCount())
                    .existingDataFilesCount(manifest.existingDataFilesCount())
                    .existingRowsCount(manifest.existingRowsCount())
                    //
                    .keyMetadata(toByteBuffer(manifest.keyMetadata()))

                    //
                    .build());
          }
        } catch (IOException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private static ByteBuffer toByteBuffer(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    return ByteBuffer.wrap(bytes);
  }

  /**
   * Retrieve the Nessie table snapshot for an {@linkplain IcebergTable Iceberg table snapshot},
   * either from the Nessie Data Catalog database or imported from the data lake into the Nessie
   * Data Catalog's database.
   */
  private NessieTableSnapshot retrieveTableSnapshot(NessieId snapshotId, ContentResponse content) {
    NessieTableSnapshot snapshot;
    CatalogEntitySnapshot catalogSnapshot = storage.loadSnapshot(snapshotId);
    if (catalogSnapshot == null) {
      snapshot = importTableSnapshot(snapshotId, content.getContent());
    } else {
      snapshot = loadTableSnapshot(catalogSnapshot);
    }
    return snapshot;
  }

  /**
   * Fetch the Nessie table snapshot from the given {@linkplain Content Nessie content object}, the
   * Nessie table snapshot does not exist in the Nessie Data Catalog database.
   */
  private NessieTableSnapshot importTableSnapshot(NessieId snapshotId, Content content) {
    if (content instanceof IcebergTable) {
      try {
        return importIcebergTableSnapshot(snapshotId, (IcebergTable) content);
      } catch (Exception e) {
        // TODO need better error handling here
        throw new RuntimeException(e);
      }
    }
    throw new UnsupportedOperationException("IMPLEMENT ME FOR " + content);
  }

  /**
   * Import the Nessie table snapshot for an {@linkplain IcebergTable Iceberg table snapshot}, from
   * the data lake into the Nessie Data Catalog's database, the Nessie table snapshot does not exist
   * in the Nessie Data Catalog database.
   */
  private NessieTableSnapshot importIcebergTableSnapshot(NessieId snapshotId, IcebergTable content)
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

    // TODO optimize all the collections and streams here
    Map<NessieId, Object> objectsToStore = new HashMap<>();
    objectsToStore.put(snapshotId, shallowSnapshot);
    objectsToStore.put(snapshot.entity().id(), snapshot.entity());
    snapshot.schemas().forEach(s -> objectsToStore.put(s.schemaId(), s));
    snapshot.partitionDefinitions().forEach(s -> objectsToStore.put(s.partitionDefinitionId(), s));
    snapshot.sortDefinitions().forEach(s -> objectsToStore.put(s.sortDefinitionId(), s));
    CatalogEntitySnapshot.Builder catalogSnapshot =
        CatalogEntitySnapshot.builder()
            .snapshotId(snapshotId)
            .entityId(snapshot.entity().id())
            .currentSchema(snapshot.currentSchema())
            .currentPartitionDefinition(snapshot.currentPartitionDefinition())
            .currentSortDefinition(snapshot.currentSortDefinition());
    snapshot.schemas().stream().map(NessieSchema::schemaId).forEach(catalogSnapshot::addSchemas);
    snapshot.partitionDefinitions().stream()
        .map(NessiePartitionDefinition::partitionDefinitionId)
        .forEach(catalogSnapshot::addPartitionDefinitions);
    snapshot.sortDefinitions().stream()
        .map(NessieSortDefinition::sortDefinitionId)
        .forEach(catalogSnapshot::addSortDefinitions);

    // Manifest list
    // TODO handling the manifest list needs to separated

    long icebergSnapshotId = tableMetadata.currentSnapshotId();
    // TODO use a memoized current snapshot instead of iterating through the list
    IntFunction<NessiePartitionDefinition> partitionDefinitionBySpecId =
        specId ->
            snapshot.partitionDefinitions().stream()
                .filter(p -> p.icebergSpecId() == specId)
                .findFirst()
                .orElse(null);

    NessieTableSnapshot.Builder snapshotBuilder = NessieTableSnapshot.builder().from(snapshot);

    Consumer<NessieListManifestEntry> listEntryConsumer =
        entry -> {
          NessieId id = NessieIdHasher.hashObject(entry);
          objectsToStore.put(id, entry);
          catalogSnapshot.addManifests(id);
          snapshotBuilder.addManifests(entry);
        };

    // TODO use a memoized current snapshot instead of iterating through the list
    tableMetadata.snapshots().stream()
        .filter(s -> s.snapshotId() == icebergSnapshotId)
        .findFirst()
        .ifPresent(snap -> importManifests(snap, partitionDefinitionBySpecId, listEntryConsumer));

    LOGGER.info(
        "Storing snapshot from Iceberg table using {} objects, base location {}",
        objectsToStore.size(),
        snapshot.icebergLocation());

    storage.createObjects(objectsToStore);
    storage.createSnapshot(catalogSnapshot.build());

    return snapshotBuilder.build();
  }

  private void importManifests(
      IcebergSnapshot icebergSnapshot,
      IntFunction<NessiePartitionDefinition> partitionDefinitionBySpecId,
      Consumer<NessieListManifestEntry> listEntryConsumer) {
    if (icebergSnapshot.manifestList() != null && !icebergSnapshot.manifestList().isEmpty()) {
      // Common code path - use the manifest-list file per Iceberg snapshot.
      importManifestListFromList(
          icebergSnapshot.manifestList(), partitionDefinitionBySpecId, listEntryConsumer);
    } else if (!icebergSnapshot.manifests().isEmpty()) {
      // Old table-metadata files, from an Iceberg version that does not write a manifest-list, but
      // just a list of manifest-file location.
      importManifestListFromFiles(icebergSnapshot.manifests());
    }
  }

  /** Imports an Iceberg manifest-list. */
  private void importManifestListFromList(
      String manifestListLocation,
      IntFunction<NessiePartitionDefinition> partitionDefinitionBySpecId,
      Consumer<NessieListManifestEntry> listEntryConsumer) {
    LOGGER.info("Fetching Iceberg manifest-list from {}", manifestListLocation);

    try (SeekableInput avroListInput =
            new SeekableStreamInput(
                manifestListLocation, () -> objectIO.readObject(URI.create(manifestListLocation)));
        IcebergManifestListReader.IcebergManifestListEntryReader listReader =
            IcebergManifestListReader.builder().build().entryReader(avroListInput)) {

      // TODO The information in the manifest-list header seems redundant?
      // TODO Do we need to store it separately?
      // TODO Do we need an assertion that it matches the values in the Iceberg snapshot?
      listReader.spec();
      listReader.snapshotId();
      listReader.parentSnapshotId();
      listReader.sequenceNumber();

      // TODO trace level
      LOGGER.info(
          "  format version {}, snapshot id {}, sequence number {}",
          listReader.spec(),
          listReader.snapshotId(),
          listReader.sequenceNumber());

      while (listReader.hasNext()) {
        IcebergManifestFile manifestListEntry = listReader.next();

        manifestListEntry.addedSnapshotId();
        manifestListEntry.sequenceNumber();
        manifestListEntry.minSequenceNumber();
        manifestListEntry.partitionSpecId();
        manifestListEntry.partitions();

        // TODO trace level
        LOGGER.info("  with manifest list entry {}", manifestListEntry);

        NessieFileContentType content;
        IcebergManifestContent entryContent = manifestListEntry.content();
        if (entryContent != null) {
          switch (entryContent) {
            case DATA:
              content = NessieFileContentType.ICEBERG_DATA_FILE;
              break;
            case DELETES:
              content = NessieFileContentType.ICEBERG_DELETE_FILE;
              break;
            default:
              throw new IllegalArgumentException();
          }
        } else {
          content = NessieFileContentType.ICEBERG_DATA_FILE;
        }

        NessiePartitionDefinition partitionDefinition =
            partitionDefinitionBySpecId.apply(manifestListEntry.partitionSpecId());

        // TODO remove collection overhead
        List<NessieFieldSummary> partitions = new ArrayList<>();
        for (int i = 0; i < manifestListEntry.partitions().size(); i++) {
          IcebergPartitionFieldSummary fieldSummary = manifestListEntry.partitions().get(i);
          partitions.add(
              NessieFieldSummary.builder()
                  .fieldId(partitionDefinition.columns().get(i).icebergFieldId())
                  .containsNan(fieldSummary.containsNan())
                  .containsNull(fieldSummary.containsNull())
                  .lowerBound(toByteArray(fieldSummary.lowerBound()))
                  .upperBound(toByteArray(fieldSummary.upperBound()))
                  .build());
        }

        NessieListManifestEntry entry =
            NessieListManifestEntry.builder()
                .content(content)
                //
                .manifestPath(manifestListEntry.manifestPath())
                .manifestLength(manifestListEntry.manifestLength())
                //
                .addedSnapshotId(manifestListEntry.addedSnapshotId())
                .sequenceNumber(manifestListEntry.sequenceNumber())
                .minSequenceNumber(manifestListEntry.minSequenceNumber())
                .partitionSpecId(manifestListEntry.partitionSpecId())
                .partitions(partitions)
                //
                .addedDataFilesCount(manifestListEntry.addedDataFilesCount())
                .addedRowsCount(manifestListEntry.addedRowsCount())
                .deletedDataFilesCount(manifestListEntry.deletedDataFilesCount())
                .deletedRowsCount(manifestListEntry.deletedRowsCount())
                .existingDataFilesCount(manifestListEntry.existingDataFilesCount())
                .existingRowsCount(manifestListEntry.existingRowsCount())
                //
                .keyMetadata(toByteArray(manifestListEntry.keyMetadata()))
                //
                .build();

        listEntryConsumer.accept(entry);

        try (SeekableInput avroFileInput =
                new SeekableStreamInput(
                    manifestListEntry.manifestPath(),
                    () -> objectIO.readObject(URI.create(manifestListEntry.manifestPath())));
            IcebergManifestFileReader.IcebergManifestFileEntryReader entryReader =
                IcebergManifestFileReader.builder().build().entryReader(avroFileInput)) {
          entryReader.schema();
          entryReader.spec();
          entryReader.content();
          entryReader.partitionSpec();
          while (entryReader.hasNext()) {
            IcebergManifestEntry manifestEntry = entryReader.next();
            manifestEntry.fileSequenceNumber();
            manifestEntry.sequenceNumber();
            manifestEntry.snapshotId();
            IcebergDataFile dataFile = manifestEntry.dataFile();
            manifestEntry.status();

            LOGGER.info("Manifest file entry: {}", manifestEntry);
          }
        }
      }

    } catch (Exception e) {
      // TODO some better error handling
      throw new RuntimeException(e);
    }
  }

  private byte[] toByteArray(ByteBuffer byteBuffer) {
    if (byteBuffer == null) {
      return null;
    }
    byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.duplicate().get(bytes);
    return bytes;
  }

  /**
   * Construct an Iceberg manifest-list from a list of manifest-file locations, used for
   * compatibility with table-metadata representations that do not have a manifest-list.
   */
  private void importManifestListFromFiles(List<String> manifests) {
    // TODO debug level
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

              dataFile.specId();
              dataFile.partition();
              dataFile.equalityIds();
              dataFile.sortOrderId();

              dataFile.fileFormat();
              dataFile.filePath();
              dataFile.fileSizeInBytes();
              dataFile.blockSizeInBytes();
              dataFile.recordCount();
              dataFile.splitOffsets();

              dataFile.keyMetadata();

              dataFile.columnSizes();
              dataFile.lowerBounds();
              dataFile.upperBounds();
              dataFile.nanValueCounts();
              dataFile.nullValueCounts();
              dataFile.valueCounts();
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

  /** Fetch requested metadata from the database, the snapshot already exists. */
  private NessieTableSnapshot loadTableSnapshot(CatalogEntitySnapshot catalogSnapshot) {
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
    ids.addAll(catalogSnapshot.manifests());

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
    catalogSnapshot.manifests().stream()
        .map(loaded::get)
        .map(NessieListManifestEntry.class::cast)
        .forEach(snapshotBuilder::addManifests);

    NessieTableSnapshot snapshot;
    snapshot = snapshotBuilder.build();
    LOGGER.info(" built: {} {}", snapshot.schemas().size(), snapshot.partitionDefinitions().size());
    return snapshot;
  }

  /** Compute the ID for the given Nessie {@link Content} object. */
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
