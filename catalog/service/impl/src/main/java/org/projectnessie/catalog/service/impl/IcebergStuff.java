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

import static java.util.concurrent.CompletableFuture.completedStage;
import static org.projectnessie.catalog.service.impl.EntitySnapshotTaskRequest.entitySnapshotTaskRequest;
import static org.projectnessie.catalog.service.impl.ManifestGroupTaskRequest.manifestGroupTaskRequest;
import static org.projectnessie.catalog.service.impl.Util.nessieIdToObjId;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.service.api.SnapshotFormat;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.nessie.tasks.api.Tasks;
import org.projectnessie.nessie.tasks.api.TasksService;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO give this class a better name
public class IcebergStuff {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergStuff.class);

  private final ObjectIO objectIO;
  private final Persist persist;
  private final TasksService tasksService;
  private final Executor executor;

  public IcebergStuff(
      ObjectIO objectIO, Persist persist, TasksService tasksService, Executor executor) {
    this.objectIO = objectIO;
    this.persist = persist;
    this.tasksService = tasksService;
    this.executor = executor;
  }

  /**
   * Retrieve the Nessie table snapshot for an {@linkplain IcebergTable Iceberg table snapshot},
   * either from the Nessie Data Catalog database or imported from the data lake into the Nessie
   * Data Catalog's database.
   */
  public CompletionStage<NessieTableSnapshot> retrieveIcebergSnapshot(
      ObjId snapshotId, Content content, SnapshotFormat format) {
    EntitySnapshotTaskRequest snapshotTaskRequest =
        entitySnapshotTaskRequest(snapshotId, content, null, persist, objectIO, executor);
    return triggerIcebergTableSnapshot(format, snapshotTaskRequest);
  }

  private CompletionStage<NessieTableSnapshot> triggerIcebergTableSnapshot(
      SnapshotFormat format, EntitySnapshotTaskRequest snapshotTaskRequest) {
    // TODO Handle hash-collision - when entity-snapshot refers to a different(!) snapshot
    return tasksService
        .forPersist(persist)
        .submit(snapshotTaskRequest)
        .thenCompose(
            snapshotObj -> {
              NessieEntitySnapshot<?> entitySnapshot = snapshotObj.snapshot();
              if (entitySnapshot instanceof NessieTableSnapshot) {
                NessieTableSnapshot tableSnapshot = (NessieTableSnapshot) entitySnapshot;
                if (format.includesFileManifestGroup()
                    && tableSnapshot.fileManifestGroup() == null) {
                  return retrieveManifestList(snapshotObj);
                }
              }
              return completedStage(mapToTableSnapshot(snapshotObj, null));
            });
  }

  public CompletionStage<NessieTableSnapshot> storeSnapshot(
      NessieTableSnapshot snapshot, Content content) {
    EntitySnapshotTaskRequest snapshotTaskRequest =
        entitySnapshotTaskRequest(
            nessieIdToObjId(snapshot.id()), content, snapshot, persist, objectIO, executor);
    return triggerIcebergTableSnapshot(SnapshotFormat.ICEBERG_MANIFEST_LIST, snapshotTaskRequest);
  }

  private CompletionStage<NessieTableSnapshot> retrieveManifestList(EntitySnapshotObj snapshotObj) {
    Tasks tasks = tasksService.forPersist(persist);
    ManifestGroupTaskRequest manifestGroupTaskRequest =
        manifestGroupTaskRequest(
            snapshotObj.manifestGroup(), snapshotObj, persist, objectIO, executor, tasks);
    // TODO Handle hash-collision - when manifest-group refers to a different(!) manifest-list
    return tasks
        .submit(manifestGroupTaskRequest)
        .thenApply(mg -> mapToTableSnapshot(snapshotObj, mg));
  }

  /** Fetch requested metadata from the database, the snapshot already exists. */
  NessieTableSnapshot mapToTableSnapshot(
      @Nonnull EntitySnapshotObj snapshotObj, @Nullable ManifestGroupObj manifestGroupObj) {
    LOGGER.debug("Fetching table snapshot from database for snapshot ID {}", snapshotObj.id());

    NessieTableSnapshot tableSnapshot = (NessieTableSnapshot) snapshotObj.snapshot();
    NessieTableSnapshot.Builder snapshotBuilder = NessieTableSnapshot.builder().from(tableSnapshot);

    if (manifestGroupObj != null) {
      snapshotBuilder.fileManifestGroup(manifestGroupObj.manifestGroup());
    }

    NessieTableSnapshot snapshot;
    snapshot = snapshotBuilder.build();
    LOGGER.debug(
        "Loaded table snapshot with {} schemas and {} partition definitions",
        snapshot.schemas().size(),
        snapshot.partitionDefinitions().size());
    return snapshot;
  }
}
