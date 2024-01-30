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

import static org.projectnessie.catalog.service.impl.ManifestTaskRequest.manifestTaskRequest;
import static org.projectnessie.catalog.service.impl.Util.nessieIdToObjId;
import static org.projectnessie.nessie.tasks.api.TaskState.successState;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.apache.avro.file.SeekableInput;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestContent;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestListReader;
import org.projectnessie.catalog.formats.iceberg.manifest.SeekableStreamInput;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionFieldSummary;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.manifest.NessieFieldSummary;
import org.projectnessie.catalog.model.manifest.NessieFileContentType;
import org.projectnessie.catalog.model.manifest.NessieFileManifestEntry;
import org.projectnessie.catalog.model.manifest.NessieFileManifestGroup;
import org.projectnessie.catalog.model.manifest.NessieFileManifestGroupEntry;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
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
      try {
        return importIcebergManifestList(
            (NessieTableSnapshot) taskRequest.snapshotObj().snapshot(), manifestGroupId);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    throw new UnsupportedOperationException("Unsupported Nessie content type " + content.getType());
  }

  private ManifestGroupObj.Builder importIcebergManifestList(
      NessieTableSnapshot snapshot, ObjId manifestGroupId) {

    NessieFileManifestGroup.Builder manifestGroup = NessieFileManifestGroup.builder();

    importIcebergManifests(snapshot, manifestGroup);

    // TODO if the manifest-list is small(-ish), we could add NessieFileManifestGroup to the
    //  NessieTableSnapshot in EntitySnapshotObj and save one database read-op at the expense of a
    //  bigger snapshot object.
    // TODO with the above, it would also be necessary to do that when the manifest-group has
    //  already been imported (i.e. another snapshot referencing the same manifest-group, which
    //  would not trigger a 2nd import, so it would not run this method!).

    return ManifestGroupObj.builder()
        .id(manifestGroupId)
        .manifestGroup(manifestGroup.build())
        .taskState(successState());
  }

  void importIcebergManifests(
      NessieTableSnapshot snapshot, NessieFileManifestGroup.Builder manifestGroup) {
    String manifestList = snapshot.icebergManifestListLocation();
    if (!snapshot.icebergManifestFileLocations().isEmpty()) {
      // Old table-metadata files, from an Iceberg version that does not write a manifest-list, but
      // just a list of manifest-file location.
      // Must test for manifest-files first, because the manifest-list location field has been
      // populated for this case.
      importManifestFilesForIcebergSpecV1(snapshot, manifestGroup);
    } else if (manifestList != null && !manifestList.isEmpty()) {
      // Common code path - use the manifest-list file per Iceberg snapshot.
      importIcebergManifestList(snapshot, manifestList, manifestGroup);
    }
  }

  /** Imports an Iceberg manifest-list. */
  void importIcebergManifestList(
      NessieTableSnapshot snapshot,
      String manifestListLocation,
      NessieFileManifestGroup.Builder manifestGroup) {
    LOGGER.debug("Fetching Iceberg manifest-list from {}", manifestListLocation);

    // TODO there is a lot of back-and-forth re-serialization:
    //  between Java-types (in Avro record) and byte(buffers)
    //  The Nessie Catalog should have ONE distinct way to serialize values and reuse data as much
    //  as possible!!

    try (SeekableInput avroListInput =
            new SeekableStreamInput(
                URI.create(manifestListLocation), taskRequest.objectIO()::readObject);
        IcebergManifestListReader.IcebergManifestListEntryReader listReader =
            IcebergManifestListReader.builder().build().entryReader(avroListInput)) {

      // TODO The information in the manifest-list header seems redundant?
      // TODO Do we need to store it separately?
      // TODO Do we need an assertion that it matches the values in the Iceberg snapshot?
      IcebergSpec listIcebergSpec = listReader.spec();
      // TODO assert if the snapshot ID's the same?
      long listSnapshotId = listReader.snapshotId();
      // TODO store parentSnapshotId in NessieTableSnapshot ?
      long listParentSnapshotId = listReader.parentSnapshotId();
      // TODO store the list's sequenceNumber separately in NessieTableSnapshot ?
      long listSequenceNumber = listReader.sequenceNumber();

      // TODO trace level
      LOGGER.debug(
          "Iceberg manifest list for format version {}, snapshot id {}, sequence number {}",
          listReader.spec(),
          listReader.snapshotId(),
          listReader.sequenceNumber());

      while (listReader.hasNext()) {
        IcebergManifestFile manifestListEntry = listReader.next();

        // TODO trace level
        LOGGER.debug(
            "Iceberg manifest list with manifest list entry for {}",
            manifestListEntry.manifestPath());

        IcebergManifestContent entryContent = manifestListEntry.content();
        NessieFileContentType content =
            entryContent != null
                ? entryContent.nessieFileContentType()
                : NessieFileContentType.ICEBERG_DATA_FILE;

        NessieId manifestId = triggerManifestFileImport(manifestListEntry, snapshot);

        NessiePartitionDefinition partitionDefinition =
            snapshot.partitionDefinitionByIcebergId().get(manifestListEntry.partitionSpecId());

        NessieFileManifestGroupEntry.Builder entry =
            NessieFileManifestGroupEntry.builder()
                .manifestId(manifestId)
                .content(content)
                //
                .icebergManifestPath(manifestListEntry.manifestPath())
                .icebergManifestLength(manifestListEntry.manifestLength())
                //
                .addedSnapshotId(manifestListEntry.addedSnapshotId())
                .sequenceNumber(manifestListEntry.sequenceNumber())
                .minSequenceNumber(manifestListEntry.minSequenceNumber())
                .partitionSpecId(manifestListEntry.partitionSpecId())
                //
                .addedDataFilesCount(manifestListEntry.addedDataFilesCount())
                .addedRowsCount(manifestListEntry.addedRowsCount())
                .deletedDataFilesCount(manifestListEntry.deletedDataFilesCount())
                .deletedRowsCount(manifestListEntry.deletedRowsCount())
                .existingDataFilesCount(manifestListEntry.existingDataFilesCount())
                .existingRowsCount(manifestListEntry.existingRowsCount())
                //
                .keyMetadata(manifestListEntry.keyMetadata());

        for (int i = 0; i < manifestListEntry.partitions().size(); i++) {
          IcebergPartitionFieldSummary fieldSummary = manifestListEntry.partitions().get(i);
          entry.addPartition(
              NessieFieldSummary.builder()
                  .fieldId(partitionDefinition.fields().get(i).icebergId())
                  .containsNan(fieldSummary.containsNan())
                  .containsNull(fieldSummary.containsNull())
                  .lowerBound(fieldSummary.lowerBound())
                  .upperBound(fieldSummary.upperBound())
                  .build());
        }

        manifestGroup.addManifest(entry.build());
      }
    } catch (Exception e) {
      // TODO some better error handling
      throw new RuntimeException(e);
    }
  }

  private NessieId triggerManifestFileImport(
      IcebergManifestFile manifestFile, NessieTableSnapshot snapshot) {
    NessieId id = NessieFileManifestEntry.id(manifestFile.manifestPath());
    taskRequest
        .tasks()
        .submit(
            manifestTaskRequest(
                nessieIdToObjId(id),
                manifestFile.manifestPath(),
                manifestFile,
                snapshot,
                taskRequest.persist(),
                taskRequest.objectIO(),
                taskRequest.executor()));
    return id;
  }

  private CompletionStage<ManifestObj> triggerManifestFileImportForIcebergSpecV1(
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
                taskRequest.executor()));
  }

  /**
   * Construct an Iceberg manifest-list from a list of manifest-file locations, used for
   * compatibility with table-metadata representations that do not have a manifest-list.
   */
  void importManifestFilesForIcebergSpecV1(
      NessieTableSnapshot snapshot, NessieFileManifestGroup.Builder manifestGroup) {

    List<String> manifests = snapshot.icebergManifestFileLocations();

    LOGGER.debug(
        "Constructing Nessie manifest group from {} manifest file locations", manifests.size());

    // For Iceberg spec V1 snapshots WITHOUT `manifest-list` we have to wait for all manifest-file
    // imports to finish to be able to construct the manifest-group entries.

    manifests.stream()
        .map(
            manifest ->
                triggerManifestFileImportForIcebergSpecV1(manifest, snapshot).toCompletableFuture())
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
  }
}
