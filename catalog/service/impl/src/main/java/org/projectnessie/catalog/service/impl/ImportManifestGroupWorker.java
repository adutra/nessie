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

import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.icebergManifestListToNessieGroup;
import static org.projectnessie.catalog.service.impl.ManifestTaskRequest.manifestTaskRequest;
import static org.projectnessie.catalog.service.impl.Util.nessieIdToObjId;
import static org.projectnessie.nessie.tasks.api.TaskState.successState;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.file.SeekableInput;
import org.projectnessie.catalog.formats.iceberg.manifest.SeekableStreamInput;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.manifest.NessieFileManifestEntry;
import org.projectnessie.catalog.model.manifest.NessieFileManifestGroup;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ImportManifestGroupWorker {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImportManifestGroupWorker.class);

  private final ManifestGroupTaskRequest taskRequest;

  ImportManifestGroupWorker(ManifestGroupTaskRequest taskRequest) {
    this.taskRequest = taskRequest;
  }

  ManifestGroupObj.Builder importManifestGroup() {
    Content content = taskRequest.snapshotObj().content();
    ObjId manifestGroupId = taskRequest.snapshotObj().manifestGroup();
    if (content instanceof IcebergTable) {
      return importIcebergManifestList(
          (NessieTableSnapshot) taskRequest.snapshotObj().snapshot(), manifestGroupId);
    }

    throw new UnsupportedOperationException("Unsupported Nessie content type " + content.getType());
  }

  private ManifestGroupObj.Builder importIcebergManifestList(
      NessieTableSnapshot snapshot, ObjId manifestGroupId) {

    NessieFileManifestGroup manifestGroup = importIcebergManifests(snapshot);

    // TODO if the manifest-list is small(-ish), we could add NessieFileManifestGroup to the
    //  NessieTableSnapshot in EntitySnapshotObj and save one database read-op at the expense of a
    //  bigger snapshot object.
    // TODO with the above, it would also be necessary to do that when the manifest-group has
    //  already been imported (i.e. another snapshot referencing the same manifest-group, which
    //  would not trigger a 2nd import, so it would not run this method!).

    return ManifestGroupObj.builder()
        .id(manifestGroupId)
        .manifestGroup(manifestGroup)
        .taskState(successState());
  }

  private NessieFileManifestGroup importIcebergManifests(NessieTableSnapshot snapshot) {
    String manifestList = snapshot.icebergManifestListLocation();
    if (!snapshot.icebergManifestFileLocations().isEmpty()) {
      // Old table-metadata files, from an Iceberg version that does not write a manifest-list, but
      // just a list of manifest-file location.
      // Must test for manifest-files first, because the manifest-list location field has been
      // populated for this case.
      return importManifestFilesForIcebergSpecV1(snapshot);
    } else if (manifestList != null && !manifestList.isEmpty()) {
      // Common code path - use the manifest-list file per Iceberg snapshot.
      return importIcebergManifestList(snapshot, manifestList);
    }
    return null;
  }

  /** Imports an Iceberg manifest-list. */
  private NessieFileManifestGroup importIcebergManifestList(
      NessieTableSnapshot snapshot, String manifestListLocation) {
    LOGGER.debug("Fetching Iceberg manifest-list from {}", manifestListLocation);

    // TODO there is a lot of back-and-forth re-serialization:
    //  between Java-types (in Avro record) and byte(buffers)
    //  The Nessie Catalog should have ONE distinct way to serialize values and reuse data as much
    //  as possible!!

    try (SeekableInput avroListInput =
        new SeekableStreamInput(
            URI.create(manifestListLocation), taskRequest.objectIO()::readObject)) {
      return icebergManifestListToNessieGroup(
          avroListInput,
          snapshot.partitionDefinitionByIcebergId()::get,
          manifestListEntry -> NessieFileManifestEntry.id(manifestListEntry.manifestPath()));
    } catch (Exception e) {
      // TODO some better error handling
      throw new RuntimeException(e);
    }
  }

  /**
   * Construct an Iceberg manifest-list from a list of manifest-file locations, used for
   * compatibility with table-metadata representations that do not have a manifest-list.
   */
  private NessieFileManifestGroup importManifestFilesForIcebergSpecV1(
      NessieTableSnapshot snapshot) {
    NessieFileManifestGroup.Builder manifestGroup = NessieFileManifestGroup.builder();

    List<String> manifests = snapshot.icebergManifestFileLocations();

    LOGGER.debug(
        "Constructing Nessie manifest group from {} manifest file locations", manifests.size());

    // For Iceberg spec V1 snapshots WITHOUT `manifest-list` we have to wait for all manifest-file
    // imports to finish to be able to construct the manifest-group entries.

    manifests.stream()
        .map(manifest -> triggerManifestFileImportForIcebergSpecV1(manifest, snapshot))
        .map(
            future -> {
              try {
                return future.get();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            })
        .map(ManifestObj.class::cast)
        .map(ManifestObj::entry)
        .forEach(manifestGroup::addManifest);

    return manifestGroup.build();
  }

  private CompletableFuture<ManifestObj> triggerManifestFileImportForIcebergSpecV1(
      String manifestFilePath, NessieTableSnapshot snapshot) {
    NessieId id = NessieFileManifestEntry.id(manifestFilePath);
    return taskRequest
        .tasks()
        .submit(
            manifestTaskRequest(
                nessieIdToObjId(id),
                manifestFilePath,
                null,
                snapshot,
                taskRequest.persist(),
                taskRequest.objectIO(),
                taskRequest.executor()))
        .toCompletableFuture();
  }
}
