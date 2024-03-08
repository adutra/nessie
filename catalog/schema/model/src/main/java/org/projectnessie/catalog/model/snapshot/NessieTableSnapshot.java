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

import static java.util.function.Function.identity;
import static org.projectnessie.catalog.model.schema.NessiePartitionDefinition.NO_PARTITION_SPEC_ID;
import static org.projectnessie.catalog.model.schema.NessieSchema.NO_SCHEMA_ID;
import static org.projectnessie.catalog.model.schema.NessieSortDefinition.NO_SORT_ORDER_ID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.manifest.NessieFileManifestGroup;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.catalog.model.schema.NessieSortDefinition;
import org.projectnessie.model.CommitMeta.InstantDeserializer;
import org.projectnessie.model.CommitMeta.InstantSerializer;
import org.projectnessie.model.Content;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.nessie.immutables.NessieImmutable;

/**
 * Represents the state of a {@link NessieTable} on a specific Nessie reference (commit).
 *
 * <p>The {@linkplain #id() ID of a table's snapshot} in the Nessie catalog is derived from relevant
 * fields in a concrete {@linkplain Content Nessie content object}, for example a {@link
 * IcebergTable} or {@link DeltaLakeTable}. This guarantees that each distinct state of a table is
 * represented by exactly one {@linkplain NessieTableSnapshot snapshot}. How exactly the {@linkplain
 * #id() ID} is derived is opaque to a user.
 */
@NessieImmutable
@JsonSerialize(as = ImmutableNessieTableSnapshot.class)
@JsonDeserialize(as = ImmutableNessieTableSnapshot.class)
@JsonTypeName("TABLE")
// Suppress: "Constructor parameters should be better defined on the same level of inheritance
// hierarchy..."
@SuppressWarnings("immutables:subtype")
public interface NessieTableSnapshot extends NessieEntitySnapshot<NessieTable> {

  NessieTableSnapshot withId(NessieId id);

  @Override
  @Value.NonAttribute
  default String type() {
    return "TABLE";
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  NessieId currentSchemaId();

  List<NessieSchema> schemas();

  @Value.Lazy
  default Map<Integer, NessieSchema> schemaByIcebergId() {
    return schemas().stream()
        .filter(s -> s.icebergId() != NO_SCHEMA_ID)
        .collect(Collectors.toMap(NessieSchema::icebergId, identity()));
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  NessieId currentPartitionDefinitionId();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<NessiePartitionDefinition> partitionDefinitions();

  @Value.Lazy
  default Map<Integer, NessiePartitionDefinition> partitionDefinitionByIcebergId() {
    return partitionDefinitions().stream()
        .filter(p -> p.icebergId() != NO_PARTITION_SPEC_ID)
        .collect(Collectors.toMap(NessiePartitionDefinition::icebergId, identity()));
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  NessieId currentSortDefinitionId();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<NessieSortDefinition> sortDefinitions();

  @Value.Lazy
  default Map<Integer, NessieSortDefinition> sortDefinitionByIcebergId() {
    return sortDefinitions().stream()
        .filter(s -> s.icebergSortOrderId() != NO_SORT_ORDER_ID)
        .collect(Collectors.toMap(NessieSortDefinition::icebergSortOrderId, identity()));
  }

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
  // Can be null, if for example no Iceberg snapshot exists in a table-metadata
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  Instant snapshotCreatedTimestamp();

  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  // FIXME is this nullable? The builder method says yes, but the interface says no.
  Instant lastUpdatedTimestamp();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  Integer icebergLastColumnId();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  Integer icebergLastPartitionId();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Map<String, String> icebergSnapshotSummary();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  String icebergLocation();

  /**
   * Corresponds to the {@code manifest-list} field in Iceberg snapshots.
   *
   * <p>When importing an Iceberg snapshot without a {@code manifest-list} but with the {@code
   * manifests} field populated, both {@link #icebergManifestFileLocations()} <em>and</em> this
   * field are populated, the manifest-list is generated from the manifest files referenced by this
   * list.
   *
   * <p>Iceberg table-metadata/snapshots generated from {@link NessieTableSnapshot} will always have
   * the {@code manifest-list} field populated and no {@code manifests} field.
   */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  String icebergManifestListLocation();

  /**
   * Corresponds to the {@code manifests} field in Iceberg snapshots.
   *
   * <p>When importing an Iceberg snapshot without a {@code manifest-list} but with the {@code
   * manifests} field populated, both this list <em>and</em> {@link #icebergManifestListLocation()}
   * are populated, the latter contains the location of the manifest-list, which is generated from
   * the manifest files referenced by this list.
   *
   * <p>Iceberg table-metadata/snapshots generated from {@link NessieTableSnapshot} will always have
   * the {@code manifest-list} field populated and no {@code manifests} field.
   */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<String> icebergManifestFileLocations();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  // TODO Store manifest-list externally, in a way that we can update the manifest-list w/o touching
  //  the persisted snapshot.
  // TODO Can we derive the ID of the manifest list from the ID of the snapshot?
  // TODO Find a way to put multiple NessieListManifestEntry in a database row.
  NessieFileManifestGroup fileManifestGroup();

  // TODO Iceberg statistics files (stored in TableMetadata - NOT in Snapshot!)
  // TODO Iceberg last updated timestamp (ms since epoch)
  // TODO Iceberg external name mapping (see org.apache.iceberg.mapping.NameMappingParser +
  //  org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING)

  @Value.Lazy
  @JsonIgnore
  @Nullable
  default NessieSchema currentSchemaObject() {
    for (NessieSchema schema : schemas()) {
      if (schema.id().equals(currentSchemaId())) {
        return schema;
      }
    }
    return null;
  }

  @Value.Lazy
  @JsonIgnore
  @Nullable
  default NessiePartitionDefinition currentPartitionDefinitionObject() {
    for (NessiePartitionDefinition partitionDefinition : partitionDefinitions()) {
      if (partitionDefinition.id().equals(currentPartitionDefinitionId())) {
        return partitionDefinition;
      }
    }
    return null;
  }

  @Value.Lazy
  @JsonIgnore
  @Nullable
  default NessieSortDefinition currentSortDefinitionObject() {
    for (NessieSortDefinition sortDefinition : sortDefinitions()) {
      if (sortDefinition.id().equals(currentSortDefinitionId())) {
        return sortDefinition;
      }
    }
    return null;
  }

  static Builder builder() {
    return ImmutableNessieTableSnapshot.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(NessieTableSnapshot instance);

    @CanIgnoreReturnValue
    Builder id(NessieId id);

    @CanIgnoreReturnValue
    Builder putProperty(String key, String value);

    @CanIgnoreReturnValue
    Builder putProperty(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    Builder properties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllProperties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder entity(NessieTable entity);

    @CanIgnoreReturnValue
    Builder currentSchemaId(@Nullable NessieId currentSchemaId);

    @CanIgnoreReturnValue
    Builder currentPartitionDefinitionId(@Nullable NessieId currentPartitionDefinitionId);

    @CanIgnoreReturnValue
    Builder currentSortDefinitionId(@Nullable NessieId currentSortDefinitionId);

    @CanIgnoreReturnValue
    Builder addSchema(NessieSchema element);

    @CanIgnoreReturnValue
    Builder addSchemas(NessieSchema... elements);

    @CanIgnoreReturnValue
    Builder schemas(Iterable<? extends NessieSchema> elements);

    @CanIgnoreReturnValue
    Builder addAllSchemas(Iterable<? extends NessieSchema> elements);

    @CanIgnoreReturnValue
    Builder addPartitionDefinition(NessiePartitionDefinition element);

    @CanIgnoreReturnValue
    Builder addPartitionDefinitions(NessiePartitionDefinition... elements);

    @CanIgnoreReturnValue
    Builder partitionDefinitions(Iterable<? extends NessiePartitionDefinition> elements);

    @CanIgnoreReturnValue
    Builder addAllPartitionDefinitions(Iterable<? extends NessiePartitionDefinition> elements);

    @CanIgnoreReturnValue
    Builder addSortDefinition(NessieSortDefinition element);

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
    Builder icebergLastColumnId(@Nullable Integer icebergLastColumnId);

    @CanIgnoreReturnValue
    Builder icebergLastPartitionId(@Nullable Integer icebergLastPartitionId);

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
    Builder addIcebergManifestFileLocation(String element);

    @CanIgnoreReturnValue
    Builder addIcebergManifestFileLocations(String... elements);

    @CanIgnoreReturnValue
    Builder icebergManifestFileLocations(Iterable<String> elements);

    @CanIgnoreReturnValue
    Builder addAllIcebergManifestFileLocations(Iterable<String> elements);

    @CanIgnoreReturnValue
    Builder fileManifestGroup(NessieFileManifestGroup fileManifestGroup);

    NessieTableSnapshot build();
  }
}
