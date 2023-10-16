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
package org.projectnessie.catalog.model.manifest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.List;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.id.NessieIdHasher;
import org.projectnessie.nessie.immutables.NessieImmutable;

// Corresponds to a manifest-list entry (aka Iceberg's ManifestFile type)
@NessieImmutable
@JsonSerialize(as = ImmutableNessieListManifestEntry.class)
@JsonDeserialize(as = ImmutableNessieListManifestEntry.class)
public interface NessieListManifestEntry {
  @Value.Default
  default NessieId id() {
    return NessieIdHasher.nessieIdHasher()
        .hash(icebergManifestPath())
        .hash(icebergManifestLength())
        .hash(partitionSpecId())
        .hash(addedSnapshotId())
        .hash(addedDataFilesCount())
        .hash(existingDataFilesCount())
        .hash(deletedDataFilesCount())
        .hash(addedRowsCount())
        .hash(existingRowsCount())
        .hash(deletedRowsCount())
        .hash(sequenceNumber())
        .hash(minSequenceNumber())
        .hash(content())
        .hash(keyMetadata())
        .hashCollection(partitions())
        .generate();
  }

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String icebergManifestPath();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Long icebergManifestLength();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Only in Iceberg
  // TODO store the NessieId or both?
  Integer partitionSpecId();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Only in Iceberg
  // TODO store the NessieId or both?
  Long addedSnapshotId();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Only in Iceberg
  Integer addedDataFilesCount();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Only in Iceberg
  Integer existingDataFilesCount();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Only in Iceberg
  Integer deletedDataFilesCount();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Only in Iceberg
  Long addedRowsCount();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Only in Iceberg
  Long existingRowsCount();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Only in Iceberg
  Long deletedRowsCount();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Only in Iceberg
  Long sequenceNumber();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Only in Iceberg
  Long minSequenceNumber();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Only in Iceberg
  NessieFileContentType content();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Only in Iceberg
  byte[] keyMetadata();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  // Only in Iceberg
  List<NessieFieldSummary> partitions();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<NessieId> dataFiles();

  static Builder builder() {
    return ImmutableNessieListManifestEntry.builder();
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder from(NessieListManifestEntry listManifestEntry);

    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder id(NessieId id);

    @CanIgnoreReturnValue
    Builder icebergManifestPath(String icebergManifestPath);

    @CanIgnoreReturnValue
    Builder icebergManifestLength(Long icebergManifestLength);

    @CanIgnoreReturnValue
    Builder partitionSpecId(Integer partitionSpecId);

    @CanIgnoreReturnValue
    Builder addedSnapshotId(@Nullable Long addedSnapshotId);

    @CanIgnoreReturnValue
    Builder addedDataFilesCount(@Nullable Integer addedDataFilesCount);

    @CanIgnoreReturnValue
    Builder existingDataFilesCount(@Nullable Integer existingDataFilesCount);

    @CanIgnoreReturnValue
    Builder deletedDataFilesCount(@Nullable Integer deletedDataFilesCount);

    @CanIgnoreReturnValue
    Builder addedRowsCount(@Nullable Long addedRowsCount);

    @CanIgnoreReturnValue
    Builder existingRowsCount(@Nullable Long existingRowsCount);

    @CanIgnoreReturnValue
    Builder deletedRowsCount(@Nullable Long existingRowsCount);

    @CanIgnoreReturnValue
    Builder sequenceNumber(@Nullable Long sequenceNumber);

    @CanIgnoreReturnValue
    Builder minSequenceNumber(@Nullable Long minSequenceNumber);

    @CanIgnoreReturnValue
    Builder content(@Nullable NessieFileContentType content);

    @CanIgnoreReturnValue
    Builder keyMetadata(@Nullable byte[] keyMetadata);

    @CanIgnoreReturnValue
    Builder addPartitions(NessieFieldSummary element);

    @CanIgnoreReturnValue
    Builder addPartitions(NessieFieldSummary... elements);

    @CanIgnoreReturnValue
    Builder partitions(Iterable<? extends NessieFieldSummary> elements);

    @CanIgnoreReturnValue
    Builder addAllPartitions(Iterable<? extends NessieFieldSummary> elements);

    @CanIgnoreReturnValue
    Builder addDataFiles(NessieId element);

    @CanIgnoreReturnValue
    Builder addDataFiles(NessieId... elements);

    @CanIgnoreReturnValue
    Builder dataFiles(Iterable<? extends NessieId> elements);

    @CanIgnoreReturnValue
    Builder addAllDataFiles(Iterable<? extends NessieId> elements);

    NessieListManifestEntry build();
  }
}
