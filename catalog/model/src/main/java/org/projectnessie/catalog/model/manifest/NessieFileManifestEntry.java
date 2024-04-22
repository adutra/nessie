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

import static org.projectnessie.catalog.model.id.NessieIdHasher.nessieIdHasher;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.nessie.immutables.NessieImmutable;

// Corresponds to one Iceberg manifest-file entry (aka Iceberg's ManifestEntry)
@NessieImmutable
@JsonSerialize(as = ImmutableNessieFileManifestEntry.class)
@JsonDeserialize(as = ImmutableNessieFileManifestEntry.class)
public interface NessieFileManifestEntry {

  @Value.Derived
  default NessieId id() {
    return id(filePath());
  }

  static NessieId id(String filePath) {
    return nessieIdHasher("NessieFileManifestEntry").hash(filePath).generate();
  }

  NessieFileStatus status();

  NessieFileContentType content();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Only in Iceberg
  // TODO store the NessieId or both?
  Integer specId();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  List<NessieFieldValue> partitionElements();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  // TODO store the NessieId for the fields or both?
  List<Integer> equalityIds();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Only in Iceberg
  // TODO store the NessieId or both?
  Integer sortOrderId();

  NessieDataFileFormat fileFormat();

  // TODO replace with BaseLocation + relative
  String filePath();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Long fileSizeInBytes();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Only for Iceberg - spec V1 only (not in V2)
  Long blockSizeInBytes();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Long recordCount();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<Long> splitOffsets();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  byte[] keyMetadata();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<NessieFieldSummary> columns();

  // Only in Delta
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Boolean tightBounds();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Only in Delta
  Boolean dataChange();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  // Only in Delta
  Map<String, String> tags();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Only in Delta
  Long deltaBaseRowId();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Only in Delta
  Long deltaDefaultRowCommitVersion();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Long icebergFileSequenceNumber();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Long icebergSequenceNumber();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Long icebergSnapshotId();

  static Builder builder() {
    return ImmutableNessieFileManifestEntry.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(NessieFileManifestEntry instance);

    @CanIgnoreReturnValue
    Builder status(NessieFileStatus status);

    @CanIgnoreReturnValue
    Builder content(NessieFileContentType content);

    @CanIgnoreReturnValue
    Builder specId(@Nullable Integer specId);

    @CanIgnoreReturnValue
    Builder addPartitionElement(NessieFieldValue element);

    @CanIgnoreReturnValue
    Builder addPartitionElements(NessieFieldValue... elements);

    @CanIgnoreReturnValue
    Builder partitionElements(@Nullable Iterable<? extends NessieFieldValue> elements);

    @CanIgnoreReturnValue
    Builder addAllPartitionElements(Iterable<? extends NessieFieldValue> elements);

    @CanIgnoreReturnValue
    Builder addEqualityId(int element);

    @CanIgnoreReturnValue
    Builder addEqualityIds(int... elements);

    @CanIgnoreReturnValue
    Builder equalityIds(Iterable<Integer> elements);

    @CanIgnoreReturnValue
    Builder addAllEqualityIds(Iterable<Integer> elements);

    @CanIgnoreReturnValue
    Builder sortOrderId(@Nullable Integer sortOrderId);

    @CanIgnoreReturnValue
    Builder fileFormat(NessieDataFileFormat fileFormat);

    @CanIgnoreReturnValue
    Builder filePath(String filePath);

    @CanIgnoreReturnValue
    Builder fileSizeInBytes(@Nullable Long fileSizeInBytes);

    @CanIgnoreReturnValue
    Builder blockSizeInBytes(@Nullable Long blockSizeInBytes);

    @CanIgnoreReturnValue
    Builder recordCount(@Nullable Long recordCount);

    @CanIgnoreReturnValue
    Builder addSplitOffset(long element);

    @CanIgnoreReturnValue
    Builder addSplitOffsets(long... elements);

    @CanIgnoreReturnValue
    Builder splitOffsets(Iterable<Long> elements);

    @CanIgnoreReturnValue
    Builder addAllSplitOffsets(Iterable<Long> elements);

    @CanIgnoreReturnValue
    Builder keyMetadata(@Nullable byte[] keyMetadata);

    @CanIgnoreReturnValue
    Builder addColumn(NessieFieldSummary element);

    @CanIgnoreReturnValue
    Builder addColumns(NessieFieldSummary... elements);

    @CanIgnoreReturnValue
    Builder columns(Iterable<? extends NessieFieldSummary> elements);

    @CanIgnoreReturnValue
    Builder addAllColumns(Iterable<? extends NessieFieldSummary> elements);

    @CanIgnoreReturnValue
    Builder tightBounds(Boolean tightBounds);

    @CanIgnoreReturnValue
    Builder dataChange(@Nullable Boolean dataChange);

    @CanIgnoreReturnValue
    Builder putTag(String key, String value);

    @CanIgnoreReturnValue
    Builder putTag(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    Builder tags(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllTags(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder deltaBaseRowId(@Nullable Long deltaBaseRowId);

    @CanIgnoreReturnValue
    Builder deltaDefaultRowCommitVersion(@Nullable Long deltaDefaultRowCommitVersion);

    @CanIgnoreReturnValue
    Builder icebergFileSequenceNumber(@Nullable Long icebergFileSequenceNumber);

    @CanIgnoreReturnValue
    Builder icebergSequenceNumber(@Nullable Long icebergSequenceNumber);

    @CanIgnoreReturnValue
    Builder icebergSnapshotId(@Nullable Long icebergSnapshotId);

    NessieFileManifestEntry build();
  }
}
