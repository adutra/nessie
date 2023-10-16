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
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.importIcebergManifests;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessieDataFileToIceberg;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessiePartitionDefinitionToIceberg;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.newNessieTable;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
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
import org.apache.avro.Schema;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergDataFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestContent;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestEntryStatus;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFileWriter;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestListWriter;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionFieldSummary;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.id.NessieIdHasher;
import org.projectnessie.catalog.model.manifest.NessieDataFileManifest;
import org.projectnessie.catalog.model.manifest.NessieListManifestEntry;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.catalog.model.schema.NessieSortDefinition;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
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
      String ref,
      ContentKey key,
      Optional<NessieId> manifestFileId,
      SnapshotFormat format,
      OptionalInt specVersion)
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
      case ICEBERG_MANIFEST_FILE:
        icebergSpec =
            specVersion.isPresent()
                ? Optional.of(IcebergSpec.forVersion(specVersion.getAsInt()))
                : Optional.empty();
        return produceIcebergManifestFile(
            fileNameBase, snapshot, manifestFileId.orElseThrow(), icebergSpec);
      default:
        throw new IllegalArgumentException("Unknown format " + format);
    }

    String fileName = fileNameBase + suffix + ".json";

    return SnapshotResponse.forEntity(result, fileName);
  }

  private SnapshotResponse produceIcebergManifestFile(
      String fileNameBase,
      NessieTableSnapshot snapshot,
      NessieId nessieId,
      Optional<IcebergSpec> icebergSpec) {
    return new SnapshotResponse() {
      @Override
      public Optional<Object> entityObject() {
        return Optional.empty();
      }

      @Override
      public String fileName() {
        return fileNameBase + "-iceberg-manifest-file.avro";
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

        NessieListManifestEntry nessieListManifestEntry =
            (NessieListManifestEntry) storage.loadObject(nessieId);

        IcebergSchema icebergSchema = NessieModelIceberg.nessieSchemaToIcebergSchema(schema);
        IcebergPartitionSpec partitionSpec =
            nessiePartitionDefinitionToIceberg(partitionDefinition);
        Schema avroPartitionSchema = partitionSpec.avroSchema(icebergSchema, "r102");

        try (IcebergManifestFileWriter.IcebergManifestFileEntryWriter writer =
            IcebergManifestFileWriter.builder()
                .spec(icebergSpec.orElse(IcebergSpec.forVersion(snapshot.icebergFormatVersion())))
                .schema(icebergSchema)
                .partitionSpec(partitionSpec)
                .addedSnapshotId(nessieListManifestEntry.addedSnapshotId())
                .sequenceNumber(nessieListManifestEntry.sequenceNumber())
                .minSequenceNumber(nessieListManifestEntry.minSequenceNumber())
                .tableProperties(snapshot.properties())
                .content(
                    IcebergManifestContent.fromNessieFileContentType(
                        nessieListManifestEntry.content()))
                .build()
                .entryWriter(outputStream, nessieListManifestEntry.icebergManifestPath())) {

          storage.loadObjects(
              nessieListManifestEntry.dataFiles(),
              (id, obj) -> {
                NessieDataFileManifest dataFileManifest = (NessieDataFileManifest) obj;
                IcebergDataFile dataFile =
                    nessieDataFileToIceberg(
                        icebergSchema, partitionSpec, dataFileManifest, avroPartitionSchema);
                writer.append(
                    dataFile,
                    IcebergManifestEntryStatus.fromNessieFileStatus(dataFileManifest.status()),
                    dataFileManifest.icebergFileSequenceNumber(),
                    dataFileManifest.icebergSequenceNumber(),
                    dataFileManifest.icebergSnapshotId());
              },
              notFound -> {
                throw new RuntimeException("not found: " + notFound);
              });
        } catch (IOException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
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
                .partitionSpec(nessiePartitionDefinitionToIceberg(partitionDefinition))
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
                                .lowerBound(p.lowerBound())
                                .upperBound(p.upperBound())
                                .build())
                    .collect(Collectors.toList());
            writer.append(
                IcebergManifestFile.builder()
                    .content(content)
                    //
                    .manifestPath(manifest.icebergManifestPath())
                    .manifestLength(manifest.icebergManifestLength())
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
                    .keyMetadata(manifest.keyMetadata())
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
      table = newNessieTable(content, tableId, tableMetadata);
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
    IntFunction<NessiePartitionDefinition> partitionDefinitionBySpecId =
        snapshot.partitionDefinitionByIcebergId()::get;

    NessieTableSnapshot.Builder snapshotBuilder = NessieTableSnapshot.builder().from(snapshot);

    Consumer<NessieListManifestEntry> listEntryConsumer =
        entry -> {
          NessieId id = entry.id();
          objectsToStore.put(id, entry);
          catalogSnapshot.addManifests(id);
          snapshotBuilder.addManifests(entry);
        };

    Consumer<NessieDataFileManifest> dataFileManifestConsumer =
        dataFile -> objectsToStore.put(dataFile.id(), dataFile);

    // TODO use a memoized current snapshot instead of iterating through the list
    tableMetadata.snapshots().stream()
        .filter(s -> s.snapshotId() == icebergSnapshotId)
        .findFirst()
        .ifPresent(
            snap ->
                importIcebergManifests(
                    snap,
                    partitionDefinitionBySpecId,
                    listEntryConsumer,
                    dataFileManifestConsumer,
                    objectIO::readObject));

    LOGGER.info(
        "Storing snapshot from Iceberg table using {} objects, base location {}",
        objectsToStore.size(),
        snapshot.icebergLocation());

    storage.createObjects(objectsToStore);
    storage.createSnapshot(catalogSnapshot.build());

    return snapshotBuilder.build();
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

    // TODO only load what's really needed
    ids.add(catalogSnapshot.entityId());
    ids.add(catalogSnapshot.snapshotId());
    ids.addAll(catalogSnapshot.schemas());
    ids.addAll(catalogSnapshot.partitionDefinitions());
    ids.addAll(catalogSnapshot.sortDefinitions());

    // TODO only load if needed
    ids.addAll(catalogSnapshot.manifests());

    Map<NessieId, Object> loaded = new HashMap<>();
    Set<NessieId> notFound = new HashSet<>();
    storage.loadObjects(ids, loaded::put, notFound::add);

    // TODO some error handling here
    checkState(notFound.isEmpty(), "Ooops - not all parts exist: %s", notFound);

    // TODO only add what's really needed
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

    // TODO only load if needed
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
