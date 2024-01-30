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
package org.projectnessie.catalog.service.impl;

import static java.util.Objects.requireNonNull;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.icebergTableSnapshotToNessie;
import static org.projectnessie.catalog.service.impl.Util.nessieIdToObjId;
import static org.projectnessie.catalog.service.impl.Util.objIdToNessieId;
import static org.projectnessie.nessie.tasks.api.TaskState.successState;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.common.persist.ObjIdHasher.objIdHasher;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergFileFormat;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.locations.BaseLocation;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.TableFormat;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ImportSnapshotWorker {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImportSnapshotWorker.class);

  private final EntitySnapshotTaskRequest taskRequest;

  ImportSnapshotWorker(EntitySnapshotTaskRequest taskRequest) {
    this.taskRequest = taskRequest;
  }

  EntitySnapshotObj.Builder importSnapshot() {
    Content content = taskRequest.content();
    if (content instanceof IcebergTable) {
      try {
        return importIcebergTable((IcebergTable) content);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    throw new UnsupportedOperationException("Unsupported Nessie content type " + content.getType());
  }

  private EntitySnapshotObj.Builder importIcebergTable(IcebergTable content) throws Exception {
    NessieId snapshotId = objIdToNessieId(taskRequest.objId());

    LOGGER.info(
        "Fetching Iceberg table metadata from object store for snapshot ID {} from {}",
        taskRequest.objId(),
        content.getMetadataLocation());

    IcebergTableMetadata tableMetadata;
    URI metadataLocation = URI.create(content.getMetadataLocation());
    try {
      tableMetadata =
          IcebergJson.objectMapper()
              .readValue(
                  taskRequest.objectIO().readObject(metadataLocation), IcebergTableMetadata.class);
    } catch (IOException e) {
      throw new IOException("Failed to read table from " + content.getMetadataLocation(), e);
    }

    ObjId entityObjId =
        objIdHasher("NessieEntity")
            .hash(requireNonNull(content.getId(), "Nessie Content has no content ID"))
            .generate();
    NessieTable table = entityObjForContent(content, tableMetadata, entityObjId);

    NessieTableSnapshot snapshot =
        icebergTableSnapshotToNessie(
            snapshotId,
            null,
            table,
            tableMetadata,
            snap -> {
              if (snap.manifestList() != null) {
                // If the Iceberg snapshot references a manifest-list, use it. This is the regular
                // case.
                return snap.manifestList();
              }

              if (snap.manifests() == null || snap.manifests().isEmpty()) {
                return null;
              }

              // If the Iceberg snapshot references manifest files and no manifest list, which can
              // happen for old Iceberg format v1 snapshots, generate a manifest list location,
              // which is then generated by the manifest-group import-worker.
              // In other words, this code "prepares" the table snapshot with a manifest-list
              // location for Iceberg snapshots that have the field `manifests` set but
              // `manifest-list` not set, as possible for Iceberg spec v1. See
              // https://iceberg.apache.org/spec/#snapshots
              // It seems impossible to have a snapshot from Iceberg that has both `manifests` and
              // `manifest-list` populated, see
              // https://github.com/apache/iceberg/pull/21/files#diff-f32244ab465f79a1dbda5582bf74404b018d044de022e047e1e9f4f9bbf32452R52-R63,
              // which the same logic as today, see
              // https://github.com/apache/iceberg/blob/8109e420e6d563a73e17ff23c321f1a48a2b976d/core/src/main/java/org/apache/iceberg/SnapshotParser.java#L79-L89.

              String listFile =
                  String.format(
                      "snap-%d-%d-%s%s",
                      snap.snapshotId(),
                      // attempt (we use a random number)
                      ThreadLocalRandom.current().nextLong(1000000, Long.MAX_VALUE),
                      // commit-id (we use a random one)
                      UUID.randomUUID(),
                      IcebergFileFormat.AVRO.fileExtension());

              return metadataLocation.resolve(listFile).toString();
            });

    ObjId manifestGroupId =
        snapshot.fileManifestGroup() == null
            ? objIdHasher("NessieManifestGroup")
                .hash(snapshot.icebergManifestListLocation())
                .generate()
            : null;

    return EntitySnapshotObj.builder()
        .id(nessieIdToObjId(snapshotId))
        .entity(entityObjId)
        .snapshot(snapshot)
        .content(content)
        .manifestGroup(manifestGroupId)
        .taskState(successState());
  }

  private NessieTable entityObjForContent(
      IcebergTable content, IcebergTableMetadata tableMetadata, ObjId entityObjId)
      throws ObjTooLargeException {
    NessieTable table;
    try {
      EntityObj entity = (EntityObj) taskRequest.persist().fetchObj(entityObjId);
      table = (NessieTable) entity.entity();
    } catch (ObjNotFoundException nf) {

      NessieTable.Builder tableBuilder =
          NessieTable.builder()
              .createdTimestamp(Instant.ofEpochMilli(tableMetadata.lastUpdatedMs()))
              .tableFormat(TableFormat.ICEBERG)
              .icebergUuid(tableMetadata.tableUuid())
              .nessieContentId(content.getId())
              .baseLocation(
                  BaseLocation.baseLocation(
                      NessieId.emptyNessieId(), "dummy", URI.create("dummy://dummy")));

      table = tableBuilder.build();

      if (taskRequest.persist().storeObj(buildEntityObj(entityObjId, table))) {
        LOGGER.debug("Persisted new entity object for content ID {}", content.getId());
      }
    }
    return table;
  }

  private static EntityObj buildEntityObj(ObjId entityObjId, NessieTable table) {
    return EntityObj.builder()
        .id(entityObjId)
        .entity(table)
        .versionToken(randomObjId().toString())
        .build();
  }
}
