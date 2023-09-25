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
package org.projectnessie.catalog.model.snapshot;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.catalog.model.schema.NessieSortDefinition;
import org.projectnessie.model.Content;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.nessie.immutables.NessieImmutable;

/**
 * Represents the state of a {@link NessieTable} on a specific Nessie reference (commit).
 *
 * <p>The {@linkplain #snapshotId() ID of a table's snapshot} in the Nessie catalog is derived from
 * relevant fields in a concrete {@linkplain Content Nessie content object}, for example a {@link
 * IcebergTable} or {@link DeltaLakeTable}. This guarantees that each distinct state of a table is
 * represented by exactly one {@linkplain NessieTableSnapshot snapshot}. How exactly the {@linkplain
 * #snapshotId() ID} is derived is opaque to a user.
 */
@NessieImmutable
@JsonSerialize(as = ImmutableNessieTableSnapshot.class)
@JsonDeserialize(as = ImmutableNessieTableSnapshot.class)
public interface NessieTableSnapshot extends NessieEntitySnapshot<NessieTable> {

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  NessieId currentSchema();

  List<NessieSchema> schemas();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  NessieId currentPartitionDefinition();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<NessiePartitionDefinition> partitionDefinitions();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  NessieId currentSortDefinition();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<NessieSortDefinition> sortDefinitions();

  @Override
  NessieTable entity();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  Integer icebergFormatVersion();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  Long icebergSnapshotId();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  Long icebergLastSequenceNumber();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  Long icebergSnapshotSequenceNumber();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  // Can be null, if for for example no Iceberg snapshot exists in a table-metadata
  Instant snapshotCreatedTimestamp();

  Instant lastUpdatedTimestamp();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Map<String, String> icebergSnapshotSummary();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  String icebergLocation();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  String icebergManifestListLocation();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<String> icebergManifestFileLocations();

  // TODO Iceberg statistics files (stored in TableMetadata - NOT in Snapshot!)
  // TODO Iceberg last updated timestamp (ms since epoch)
  // TODO Iceberg external name mapping (see org.apache.iceberg.mapping.NameMappingParser +
  //  org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING)

  static Builder builder() {
    return ImmutableNessieTableSnapshot.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(NessieTableSnapshot instance);

    @CanIgnoreReturnValue
    Builder snapshotId(NessieId snapshotId);

    @CanIgnoreReturnValue
    Builder putProperties(String key, String value);

    @CanIgnoreReturnValue
    Builder putProperties(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    Builder properties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllProperties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder entity(NessieTable entity);

    @CanIgnoreReturnValue
    Builder currentSchema(@Nullable NessieId currentSchema);

    @CanIgnoreReturnValue
    Builder currentPartitionDefinition(@Nullable NessieId currentPartitionDefinition);

    @CanIgnoreReturnValue
    Builder currentSortDefinition(@Nullable NessieId currentSortDefinition);

    @CanIgnoreReturnValue
    Builder addSchemas(NessieSchema element);

    @CanIgnoreReturnValue
    Builder addSchemas(NessieSchema... elements);

    @CanIgnoreReturnValue
    Builder schemas(Iterable<? extends NessieSchema> elements);

    @CanIgnoreReturnValue
    Builder addAllSchemas(Iterable<? extends NessieSchema> elements);

    @CanIgnoreReturnValue
    Builder addPartitionDefinitions(NessiePartitionDefinition element);

    @CanIgnoreReturnValue
    Builder addPartitionDefinitions(NessiePartitionDefinition... elements);

    @CanIgnoreReturnValue
    Builder partitionDefinitions(Iterable<? extends NessiePartitionDefinition> elements);

    @CanIgnoreReturnValue
    Builder addAllPartitionDefinitions(Iterable<? extends NessiePartitionDefinition> elements);

    @CanIgnoreReturnValue
    Builder addSortDefinitions(NessieSortDefinition element);

    @CanIgnoreReturnValue
    Builder addSortDefinitions(NessieSortDefinition... elements);

    @CanIgnoreReturnValue
    Builder sortDefinitions(Iterable<? extends NessieSortDefinition> elements);

    @CanIgnoreReturnValue
    Builder addAllSortDefinitions(Iterable<? extends NessieSortDefinition> elements);

    @CanIgnoreReturnValue
    Builder icebergFormatVersion(@Nullable Integer icebergFormatVersion);

    @CanIgnoreReturnValue
    Builder icebergSnapshotId(@Nullable Long icebergSnapshotId);

    @CanIgnoreReturnValue
    Builder icebergLastSequenceNumber(@Nullable Long icebergLastSequenceNumber);

    @CanIgnoreReturnValue
    Builder icebergSnapshotSequenceNumber(@Nullable Long icebergSnapshotSequenceNumber);

    @CanIgnoreReturnValue
    Builder snapshotCreatedTimestamp(@Nullable Instant snapshotCreatedTimestamp);

    @CanIgnoreReturnValue
    Builder lastUpdatedTimestamp(@Nullable Instant lastUpdatedTimestamp);

    @CanIgnoreReturnValue
    Builder putIcebergSnapshotSummary(String key, String value);

    @CanIgnoreReturnValue
    Builder putIcebergSnapshotSummary(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    Builder icebergSnapshotSummary(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllIcebergSnapshotSummary(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder icebergLocation(String icebergLocation);

    @CanIgnoreReturnValue
    Builder icebergManifestListLocation(
        @jakarta.annotation.Nullable String icebergManifestListLocation);

    @CanIgnoreReturnValue
    Builder addIcebergManifestFileLocations(String element);

    @CanIgnoreReturnValue
    Builder addIcebergManifestFileLocations(String... elements);

    @CanIgnoreReturnValue
    Builder icebergManifestFileLocations(Iterable<String> elements);

    @CanIgnoreReturnValue
    Builder addAllIcebergManifestFileLocations(Iterable<String> elements);

    NessieTableSnapshot build();
  }
}
