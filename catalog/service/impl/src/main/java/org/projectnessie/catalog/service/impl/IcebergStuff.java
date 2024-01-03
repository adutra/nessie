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
import static java.util.Objects.requireNonNull;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.icebergTableSnapshotToNessie;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.importIcebergManifests;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.newNessieTable;
import static org.projectnessie.catalog.model.id.NessieIdHasher.nessieIdHasher;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.id.NessieIdHasher;
import org.projectnessie.catalog.model.manifest.NessieFileManifestEntry;
import org.projectnessie.catalog.model.manifest.NessieFileManifestGroup;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.catalog.model.schema.NessieSortDefinition;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.storage.backend.CatalogEntitySnapshot;
import org.projectnessie.catalog.storage.backend.CatalogStorage;
import org.projectnessie.catalog.storage.backend.ObjectMismatchException;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO give this class a better name
public class IcebergStuff {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergStuff.class);

  private final ObjectIO objectIO;
  private final CatalogStorage storage;

  public IcebergStuff(ObjectIO objectIO, CatalogStorage storage) {
    this.objectIO = objectIO;
    this.storage = storage;
  }

  /**
   * Retrieve the Nessie table snapshot for an {@linkplain IcebergTable Iceberg table snapshot},
   * either from the Nessie Data Catalog database or imported from the data lake into the Nessie
   * Data Catalog's database.
   */
  public NessieTableSnapshot retrieveIcebergSnapshot(NessieId snapshotId, Content content) {
    NessieTableSnapshot snapshot;
    CatalogEntitySnapshot catalogSnapshot = storage.loadSnapshot(snapshotId);
    if (catalogSnapshot == null) {
      snapshot = importTableSnapshot(snapshotId, content);
    } else {
      snapshot = loadTableSnapshot(catalogSnapshot);
    }
    return snapshot;
  }

  /**
   * Fetch the Nessie table snapshot from the given {@linkplain Content Nessie content object}, the
   * Nessie table snapshot does not exist in the Nessie Data Catalog database.
   */
  NessieTableSnapshot importTableSnapshot(NessieId snapshotId, Content content) {
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
  NessieTableSnapshot importIcebergTableSnapshot(NessieId snapshotId, IcebergTable content)
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

    NessieId tableId =
        NessieId.nessieIdFromUUID(
            UUID.fromString(requireNonNull(content.getId(), "Nessie Content has no content ID")));
    NessieTable table = (NessieTable) storage.loadObject(tableId);

    if (table == null) {
      table = newNessieTable(content, tableId, tableMetadata);
    }

    NessieTableSnapshot snapshot =
        icebergTableSnapshotToNessie(snapshotId, null, table, tableMetadata);

    // FIXME isn't CatalogEntitySnapshot already a shallow version of NessieTableSnapshot?
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
    snapshot.schemas().forEach(s -> objectsToStore.put(s.id(), s));
    snapshot.partitionDefinitions().forEach(s -> objectsToStore.put(s.id(), s));
    snapshot.sortDefinitions().forEach(s -> objectsToStore.put(s.sortDefinitionId(), s));
    CatalogEntitySnapshot.Builder catalogSnapshot =
        CatalogEntitySnapshot.builder()
            .snapshotId(snapshotId)
            .entityId(snapshot.entity().id())
            .currentSchema(snapshot.currentSchema())
            .currentPartitionDefinition(snapshot.currentPartitionDefinition())
            .currentSortDefinition(snapshot.currentSortDefinition());
    snapshot.schemas().stream().map(NessieSchema::id).forEach(catalogSnapshot::addSchemas);
    snapshot.partitionDefinitions().stream()
        .map(NessiePartitionDefinition::id)
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

    Consumer<NessieFileManifestEntry> dataFileManifestConsumer =
        dataFile -> objectsToStore.put(dataFile.id(), dataFile);

    // TODO use a memoized current snapshot instead of iterating through the list
    tableMetadata.snapshots().stream()
        .filter(s -> s.snapshotId() == icebergSnapshotId)
        .findFirst()
        .map(
            snap -> {
              NessieIdHasher fileManifestGroupId = nessieIdHasher("NessieFileManifestGroup");

              NessieFileManifestGroup.Builder fileManifestGroupBuilder =
                  NessieFileManifestGroup.builder();

              importIcebergManifests(
                  snap,
                  partitionDefinitionBySpecId,
                  fileManifestGroupId,
                  fileManifestGroupBuilder::addManifest,
                  dataFileManifestConsumer,
                  objectIO::readObject);

              fileManifestGroupBuilder.id(fileManifestGroupId.generate());

              return fileManifestGroupBuilder.build();
            })
        .ifPresent(
            fileManifestGroup -> {
              objectsToStore.put(fileManifestGroup.id(), fileManifestGroup);
              catalogSnapshot.fileManifestGroup(fileManifestGroup.id());
              snapshotBuilder.fileManifestGroup(fileManifestGroup);
            });

    LOGGER.info(
        "Storing snapshot from Iceberg table using {} objects, base location {}",
        objectsToStore.size(),
        snapshot.icebergLocation());

    for (Map.Entry<NessieId, Object> e : objectsToStore.entrySet()) {
      LOGGER.warn("STORING {} : {}", e.getKey(), e.getValue().getClass().getName());
    }

    storage.createObjects(objectsToStore);
    storage.createSnapshot(catalogSnapshot.build());

    return snapshotBuilder.build();
  }

  /** Fetch requested metadata from the database, the snapshot already exists. */
  NessieTableSnapshot loadTableSnapshot(CatalogEntitySnapshot catalogSnapshot) {
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
    if (catalogSnapshot.fileManifestGroup() != null) {
      ids.add(catalogSnapshot.fileManifestGroup());
    }

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

    NessieFileManifestGroup fileManifestGroup =
        (NessieFileManifestGroup) loaded.get(catalogSnapshot.fileManifestGroup());
    snapshotBuilder.fileManifestGroup(fileManifestGroup);

    NessieTableSnapshot snapshot;
    snapshot = snapshotBuilder.build();
    LOGGER.info(" built: {} {}", snapshot.schemas().size(), snapshot.partitionDefinitions().size());
    return snapshot;
  }
}
