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

import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.icebergTableSnapshotToNessie;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.importIcebergManifests;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.newNessieTable;
import static org.projectnessie.catalog.model.id.NessieIdHasher.nessieIdHasher;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
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
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.service.api.SnapshotFormat;
import org.projectnessie.catalog.service.storage.EntityObj;
import org.projectnessie.catalog.service.storage.EntitySnapshotObj;
import org.projectnessie.catalog.service.storage.ManifestEntryObj;
import org.projectnessie.catalog.service.storage.ManifestGroupObj;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO give this class a better name
public class IcebergStuff {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergStuff.class);

  private final ObjectIO objectIO;
  private final Persist persist;

  public IcebergStuff(ObjectIO objectIO, Persist persist) {
    this.objectIO = objectIO;
    this.persist = persist;
  }

  static ObjId nessieIdToObjId(NessieId id) {
    return ObjId.objIdFromByteArray(id.idAsBytes());
  }

  static NessieId objIdToNessieId(ObjId id) {
    return NessieId.nessieIdFromBytes(id.asByteArray());
  }

  /**
   * Retrieve the Nessie table snapshot for an {@linkplain IcebergTable Iceberg table snapshot},
   * either from the Nessie Data Catalog database or imported from the data lake into the Nessie
   * Data Catalog's database.
   */
  public NessieTableSnapshot retrieveIcebergSnapshot(
      NessieId snapshotId, Content content, SnapshotFormat format) {
    NessieTableSnapshot snapshot;
    try {
      EntitySnapshotObj snapshotObj =
          (EntitySnapshotObj) persist.fetchObj(nessieIdToObjId(snapshotId));
      return loadTableSnapshot(snapshotObj, format);
    } catch (ObjNotFoundException e) {
      return importTableSnapshot(snapshotId, content, format);
    }
  }

  /**
   * Fetch the Nessie table snapshot from the given {@linkplain Content Nessie content object}, the
   * Nessie table snapshot does not exist in the Nessie Data Catalog database.
   */
  NessieTableSnapshot importTableSnapshot(
      NessieId snapshotId, Content content, SnapshotFormat format) {
    if (content instanceof IcebergTable) {
      try {
        return importIcebergTableSnapshot(snapshotId, (IcebergTable) content, format);
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
  NessieTableSnapshot importIcebergTableSnapshot(
      NessieId snapshotId, IcebergTable content, SnapshotFormat format) throws IOException {
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

    List<Obj> objectsToStore = new ArrayList<>();

    NessieId tableId =
        NessieId.nessieIdFromUUID(
            UUID.fromString(requireNonNull(content.getId(), "Nessie Content has no content ID")));
    NessieTable table;
    ObjId entityObjId = nessieIdToObjId(tableId);
    try {
      EntityObj tableObj = (EntityObj) persist.fetchObj(entityObjId);
      table = (NessieTable) tableObj.entity();
    } catch (ObjNotFoundException nf) {
      table = newNessieTable(content, tableId, tableMetadata);
      objectsToStore.add(EntityObj.builder().id(entityObjId).entity(table).build());
    }

    NessieTableSnapshot snapshot =
        icebergTableSnapshotToNessie(snapshotId, null, table, tableMetadata);

    EntitySnapshotObj.Builder entitySnapshot =
        EntitySnapshotObj.builder().id(nessieIdToObjId(snapshotId)).entity(entityObjId);

    // Manifest list
    // TODO handling the manifest list needs to separated

    long icebergSnapshotId = tableMetadata.currentSnapshotId();
    IntFunction<NessiePartitionDefinition> partitionDefinitionBySpecId =
        snapshot.partitionDefinitionByIcebergId()::get;
    IntFunction<NessieSchema> schemaBySchemaId = snapshot.schemaByIcebergId()::get;

    NessieTableSnapshot.Builder snapshotBuilder = NessieTableSnapshot.builder().from(snapshot);

    Consumer<NessieFileManifestEntry> dataFileManifestConsumer =
        dataFile ->
            objectsToStore.add(
                ManifestEntryObj.builder()
                    .id(nessieIdToObjId(dataFile.id()))
                    .entry(dataFile)
                    .build());

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
                  tableMetadata,
                  snap,
                  schemaBySchemaId,
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
              ObjId manifestGroupId = nessieIdToObjId(fileManifestGroup.id());
              objectsToStore.add(
                  ManifestGroupObj.builder()
                      .id(manifestGroupId)
                      .manifestGroup(fileManifestGroup)
                      .build());
              entitySnapshot.manifestGroup(manifestGroupId);
              if (format.includesFileManifestGroup()) {
                snapshotBuilder.fileManifestGroup(fileManifestGroup);
              }
            });

    LOGGER.info(
        "Storing snapshot from Iceberg table using {} objects, base location {}",
        objectsToStore.size(),
        snapshot.icebergLocation());

    for (Obj e : objectsToStore) {
      LOGGER.warn("STORING {} : {}", e.id(), e.getClass().getName());
    }

    try {
      persist.storeObjs(objectsToStore.toArray(new Obj[0]));
    } catch (ObjTooLargeException e) {
      // TODO do something?
      throw new RuntimeException(e);
    }

    return snapshotBuilder.build();
  }

  /** Fetch requested metadata from the database, the snapshot already exists. */
  NessieTableSnapshot loadTableSnapshot(EntitySnapshotObj snapshotObj, SnapshotFormat format) {
    // TODO debug level
    LOGGER.info("Fetching table snapshot from database for snapshot ID {}", snapshotObj.id());

    List<ObjId> ids = new ArrayList<>();

    // TODO only load what's really needed
    ids.add(snapshotObj.entity());

    // TODO only load if needed
    if (format.includesFileManifestGroup() && snapshotObj.manifestGroup() != null) {
      ids.add(snapshotObj.manifestGroup());
    }

    Map<ObjId, Object> loaded;
    try {
      loaded =
          Arrays.stream(persist.fetchObjs(ids.toArray(new ObjId[0])))
              .collect(Collectors.toMap(Obj::id, identity()));
    } catch (ObjNotFoundException e) {
      throw new RuntimeException(e);
    }

    // TODO only add what's really needed
    NessieTableSnapshot.Builder snapshotBuilder =
        NessieTableSnapshot.builder()
            .from((NessieTableSnapshot) snapshotObj.snapshot())
            .entity((NessieTable) loaded.get(snapshotObj.entity()));

    if (format.includesFileManifestGroup()) {
      NessieFileManifestGroup fileManifestGroup =
          (NessieFileManifestGroup) loaded.get(snapshotObj.manifestGroup());
      snapshotBuilder.fileManifestGroup(fileManifestGroup);
    }

    NessieTableSnapshot snapshot;
    snapshot = snapshotBuilder.build();
    LOGGER.info(" built: {} {}", snapshot.schemas().size(), snapshot.partitionDefinitions().size());
    return snapshot;
  }
}
