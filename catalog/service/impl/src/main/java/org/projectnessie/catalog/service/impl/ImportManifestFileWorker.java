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

import static com.google.common.base.Preconditions.checkState;
import static org.projectnessie.catalog.service.impl.Util.objIdToNessieId;
import static org.projectnessie.nessie.tasks.api.TaskState.successState;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergColumnStatsCollector;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergDataFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestContent;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestEntry;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestEntryStatus;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFileReader;
import org.projectnessie.catalog.formats.iceberg.manifest.SeekableStreamInput;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionField;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.model.manifest.NessieFieldSummary;
import org.projectnessie.catalog.model.manifest.NessieFieldValue;
import org.projectnessie.catalog.model.manifest.NessieFileContentType;
import org.projectnessie.catalog.model.manifest.NessieFileManifestEntry;
import org.projectnessie.catalog.model.manifest.NessieFileManifestGroupEntry;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessiePartitionField;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ImportManifestFileWorker {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImportManifestFileWorker.class);

  private final ManifestTaskRequest taskRequest;
  private final NessieTableSnapshot snapshot;

  public ImportManifestFileWorker(ManifestTaskRequest taskRequest, NessieTableSnapshot snapshot) {
    this.taskRequest = taskRequest;
    this.snapshot = snapshot;
  }

  ManifestObj.Builder importManifestFile(String location) {
    try {
      NessieFileManifestGroupEntry groupEntry =
          nessieFileManifestGroupEntryFromIcebergManifestFile(location);
      return ManifestObj.builder()
          .id(taskRequest.objId())
          .entry(groupEntry)
          .taskState(successState());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Produces a {@link NessieFileManifestGroupEntry} from a manifest file. */
  NessieFileManifestGroupEntry nessieFileManifestGroupEntryFromIcebergManifestFile(String location)
      throws Exception {

    LOGGER.debug("Importing Iceberg manifest file {}", location);

    try (SeekableInput avroFileInput =
            new SeekableStreamInput(URI.create(location), taskRequest.objectIO()::readObject);
        IcebergManifestFileReader.IcebergManifestFileEntryReader entryReader =
            IcebergManifestFileReader.builder().build().entryReader(avroFileInput)) {

      entryReader.spec();

      IcebergManifestContent entryContent = entryReader.content();
      NessieFileContentType content =
          entryContent != null
              ? entryContent.nessieFileContentType()
              : NessieFileContentType.ICEBERG_DATA_FILE;

      IcebergPartitionSpec partitionSpec = entryReader.partitionSpec();
      IcebergSchema schema = entryReader.schema();

      NessiePartitionDefinition partitionDefinition =
          snapshot.partitionDefinitionByIcebergId().get(partitionSpec.specId());

      NessieFileManifestGroupEntry.Builder entry =
          NessieFileManifestGroupEntry.builder()
              // points to itself, mandatory attribute
              .manifestId(objIdToNessieId(taskRequest.objId()))
              .content(content)
              .partitionSpecId(entryReader.partitionSpec().specId());

      IcebergManifestFile manifestFile = taskRequest.manifestFile();
      if (manifestFile != null) {
        entry
            .icebergManifestPath(manifestFile.manifestPath())
            .icebergManifestLength(manifestFile.manifestLength())
            //
            .addedSnapshotId(manifestFile.addedSnapshotId())
            .sequenceNumber(manifestFile.sequenceNumber())
            .minSequenceNumber(manifestFile.minSequenceNumber())
            // TODO already set above
            // .partitionSpecId(manifestFile.partitionSpecId())
            //
            // TODO the following block is probably duplicate effort, since
            //  addToNessieFileManifestGroupEntryBuilder() does it as well
            .addedDataFilesCount(manifestFile.addedDataFilesCount())
            .addedRowsCount(manifestFile.addedRowsCount())
            .deletedDataFilesCount(manifestFile.deletedDataFilesCount())
            .deletedRowsCount(manifestFile.deletedRowsCount())
            .existingDataFilesCount(manifestFile.existingDataFilesCount())
            .existingRowsCount(manifestFile.existingRowsCount())
            //
            .keyMetadata(manifestFile.keyMetadata());
      } else {
        entry
            .icebergManifestPath(taskRequest.manifestFileLocation())
            .icebergManifestLength(avroFileInput.length())
            //
            .addedSnapshotId(snapshot.icebergSnapshotId())
        //
        // TODO?? .sequenceNumber(manifestListEntry.sequenceNumber())
        // TODO?? .minSequenceNumber(manifestListEntry.minSequenceNumber())
        //
        // TODO?? .keyMetadata(manifestListEntry.keyMetadata())
        ;
      }

      IcebergColumnStatsCollector statsCollector =
          new IcebergColumnStatsCollector(schema, partitionSpec);

      long minSeqNum = Long.MAX_VALUE;
      long seqNum = 0L;
      while (entryReader.hasNext()) {
        IcebergManifestEntry manifestEntry = entryReader.next();
        Long sequenceNumber = manifestEntry.sequenceNumber();
        if (sequenceNumber != null) {
          // TODO is this correct??
          minSeqNum = Math.min(minSeqNum, sequenceNumber);
          seqNum = Math.max(seqNum, sequenceNumber);
        }
        manifestEntry.snapshotId();
        manifestEntry.fileSequenceNumber();

        statsCollector.addManifestEntry(manifestEntry);

        IcebergDataFile dataFile = manifestEntry.dataFile();
        IcebergManifestEntryStatus status = manifestEntry.status();

        NessieFileManifestEntry.Builder fileManifestBuilder =
            NessieFileManifestEntry.builder()
                .icebergFileSequenceNumber(manifestEntry.fileSequenceNumber())
                // TODO ?? .icebergSequenceNumber(manifestEntry.sequenceNumber())
                .icebergSnapshotId(manifestEntry.snapshotId())
                .status(status.nessieFileStatus());

        NessieFileManifestEntry nessieDataFile =
            icebergDataFileToNessieFileManifestEntry(
                fileManifestBuilder, dataFile, partitionSpec, schema, partitionDefinition);

        // TODO append `nessieDataFile` to a "huge" persisted object containing all
        //  manifest-file-entries.

        LOGGER.debug(
            "Generated new manifest file entry:\n    {}\n    nessieDataFile: {}",
            manifestEntry,
            nessieDataFile);
      }

      statsCollector.addToNessieFileManifestGroupEntryBuilder(entry);

      return entry.build();
    }
  }

  static NessieFileManifestEntry icebergDataFileToNessieFileManifestEntry(
      NessieFileManifestEntry.Builder dataFileManifestBuilder,
      IcebergDataFile dataFile,
      IcebergPartitionSpec partitionSpec,
      IcebergSchema schema,
      NessiePartitionDefinition partitionDefinition) {
    // TODO map to Nessie IDs
    // TODO isn't this information redundant to the information in the manifest-list-entry?
    // TODO should we add the sort-order-ID to the Nessie Catalog's manifest-list-entry?
    dataFileManifestBuilder.specId(dataFile.specId()).sortOrderId(dataFile.sortOrderId());

    dataFileManifestBuilder
        .content(dataFile.content().nessieFileContentType())
        .fileFormat(dataFile.fileFormat().nessieDataFileFormat())
        .filePath(dataFile.filePath())
        .fileSizeInBytes(dataFile.fileSizeInBytes())
        .blockSizeInBytes(dataFile.blockSizeInBytes())
        .recordCount(dataFile.recordCount());
    if (dataFile.splitOffsets() != null) {
      dataFileManifestBuilder.splitOffsets(dataFile.splitOffsets());
    }

    GenericData.Record partition = dataFile.partition(); // Avro data record
    for (int i = 0; i < partitionSpec.fields().size(); i++) {
      IcebergPartitionField partitionField = partitionSpec.fields().get(i);
      byte[] serializedPartitionField =
          partitionField.type(schema).serializeSingleValue(partition.get(i));
      NessiePartitionField nessiePartitionField =
          partitionDefinition.icebergColumnMap().get(partitionField.fieldId());
      checkState(
          nessiePartitionField != null,
          "No partition field in definition for Iceberg field ID %s",
          partitionField.fieldId());
      dataFileManifestBuilder.addPartitionElement(
          NessieFieldValue.builder()
              .fieldId(nessiePartitionField.id())
              .value(serializedPartitionField)
              .icebergFieldId(partitionField.fieldId())
              .build());
    }

    dataFileStatsToFieldSummaries(dataFile, dataFileManifestBuilder);

    dataFileManifestBuilder.keyMetadata(dataFile.keyMetadata());

    return dataFileManifestBuilder.build();
  }

  private static void dataFileStatsToFieldSummaries(
      IcebergDataFile dataFile, NessieFileManifestEntry.Builder dataFileManifestBuilder) {
    // Unlike Iceberg, Nessie Catalog stores a field-summary object, so one object per field
    // in a single map, not many maps.
    Map<Integer, NessieFieldSummary.Builder> fieldSummaries = new HashMap<>();

    fieldSummaryApply(
        fieldSummaries, dataFile.columnSizes(), NessieFieldSummary.Builder::columnSize);
    fieldSummaryApply(
        fieldSummaries, dataFile.lowerBounds(), NessieFieldSummary.Builder::lowerBound);
    fieldSummaryApply(
        fieldSummaries, dataFile.upperBounds(), NessieFieldSummary.Builder::upperBound);
    fieldSummaryApply(
        fieldSummaries, dataFile.nanValueCounts(), NessieFieldSummary.Builder::nanValueCount);
    fieldSummaryApply(
        fieldSummaries, dataFile.nullValueCounts(), NessieFieldSummary.Builder::nullValueCount);
    fieldSummaryApply(
        fieldSummaries, dataFile.valueCounts(), NessieFieldSummary.Builder::valueCount);

    fieldSummaries.values().stream()
        .map(NessieFieldSummary.Builder::build)
        .forEach(dataFileManifestBuilder::addColumns);
  }

  private static <V> void fieldSummaryApply(
      Map<Integer, NessieFieldSummary.Builder> fieldSummaries,
      Map<Integer, V> attributeMap,
      BiConsumer<NessieFieldSummary.Builder, V> valueConsumer) {
    for (Map.Entry<Integer, V> e : attributeMap.entrySet()) {
      Integer id = e.getKey();

      NessieFieldSummary.Builder builder =
          fieldSummaries.computeIfAbsent(
              id,
              i ->
                  NessieFieldSummary.builder()
                      // TODO should use the Nessie ID for the field here - TAKE CARE:
                      //  the field IDs may only be present in another schema (think:
                      //  dropped column)
                      .fieldId(i));

      valueConsumer.accept(builder, e.getValue());
    }
    attributeMap.forEach((key, value) -> {});
  }
}
