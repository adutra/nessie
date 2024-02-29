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
package org.projectnessie.catalog.formats.iceberg.meta;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;
import javax.annotation.Nullable;
import org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.nessie.immutables.NessieImmutable;

/** Iceberg metadata update objects serialized according to the Iceberg REST Catalog schema. */
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "action")
@JsonSubTypes({
  @JsonSubTypes.Type(value = IcebergMetadataUpdate.AssignUUID.class, name = "assign-uuid"),
  @JsonSubTypes.Type(value = IcebergMetadataUpdate.AddSchema.class, name = "add-schema"),
  @JsonSubTypes.Type(value = IcebergMetadataUpdate.AddPartitionSpec.class, name = "add-spec"),
  @JsonSubTypes.Type(value = IcebergMetadataUpdate.AddSortOrder.class, name = "add-sort-order"),
  @JsonSubTypes.Type(value = IcebergMetadataUpdate.AddSnapshot.class, name = "add-snapshot"),
  @JsonSubTypes.Type(value = IcebergMetadataUpdate.SetLocation.class, name = "set-location"),
  @JsonSubTypes.Type(value = IcebergMetadataUpdate.SetProperties.class, name = "set-properties"),
  @JsonSubTypes.Type(value = IcebergMetadataUpdate.SetSnapshotRef.class, name = "set-snapshot-ref"),
  // TODO: UpgradeFormatVersion
  // TODO: SetCurrentSchema
  // TODO: SetDefaultPartitionSpec
  // TODO: SetDefaultSortOrder
  // TODO: SetStatistics
  // TODO: RemoveStatistics
  // TODO: RemoveSnapshot
  // TODO: RemoveSnapshotRef
  // TODO: RemoveProperties
  // TODO: AddViewVersion
  // TODO: SetCurrentViewVersion
})
public interface IcebergMetadataUpdate {

  void apply(NessieTableSnapshot.Builder builder, NessieTableSnapshot snapshot);

  @NessieImmutable
  @JsonSerialize(as = ImmutableAssignUUID.class)
  @JsonDeserialize(as = ImmutableAssignUUID.class)
  interface AssignUUID extends IcebergMetadataUpdate {
    String uuid();

    @Override
    default void apply(NessieTableSnapshot.Builder builder, NessieTableSnapshot snapshot) {
      NessieModelIceberg.assignUUID(this, builder, snapshot);
    }
  }

  @NessieImmutable
  @JsonSerialize(as = ImmutableAddSchema.class)
  @JsonDeserialize(as = ImmutableAddSchema.class)
  interface AddSchema extends IcebergMetadataUpdate {
    IcebergSchema schema();

    int lastColumnId();

    @Override
    default void apply(NessieTableSnapshot.Builder builder, NessieTableSnapshot snapshot) {
      NessieModelIceberg.addSchema(this, builder, snapshot);
    }
  }

  @NessieImmutable
  @JsonSerialize(as = ImmutableAddPartitionSpec.class)
  @JsonDeserialize(as = ImmutableAddPartitionSpec.class)
  interface AddPartitionSpec extends IcebergMetadataUpdate {
    IcebergPartitionSpec spec();

    @Override
    default void apply(NessieTableSnapshot.Builder builder, NessieTableSnapshot snapshot) {
      NessieModelIceberg.addPartitionSpec(this, builder, snapshot);
    }
  }

  @NessieImmutable
  @JsonSerialize(as = ImmutableAddSnapshot.class)
  @JsonDeserialize(as = ImmutableAddSnapshot.class)
  interface AddSnapshot extends IcebergMetadataUpdate {
    IcebergSnapshot snapshot();

    @Override
    default void apply(NessieTableSnapshot.Builder builder, NessieTableSnapshot snapshot) {
      NessieModelIceberg.addSnapshot(this, builder, snapshot);
    }
  }

  @NessieImmutable
  @JsonSerialize(as = ImmutableAddSortOrder.class)
  @JsonDeserialize(as = ImmutableAddSortOrder.class)
  interface AddSortOrder extends IcebergMetadataUpdate {
    IcebergSortOrder sortOrder();

    @Override
    default void apply(NessieTableSnapshot.Builder builder, NessieTableSnapshot snapshot) {
      NessieModelIceberg.addSortOrder(this, builder, snapshot);
    }
  }

  @NessieImmutable
  @JsonSerialize(as = ImmutableSetLocation.class)
  @JsonDeserialize(as = ImmutableSetLocation.class)
  interface SetLocation extends IcebergMetadataUpdate {
    String location();

    @Override
    default void apply(NessieTableSnapshot.Builder builder, NessieTableSnapshot snapshot) {
      NessieModelIceberg.setLocation(this, builder, snapshot);
    }
  }

  @NessieImmutable
  @JsonSerialize(as = ImmutableSetProperties.class)
  @JsonDeserialize(as = ImmutableSetProperties.class)
  interface SetProperties extends IcebergMetadataUpdate {
    Map<String, String> updates();

    @Override
    default void apply(NessieTableSnapshot.Builder builder, NessieTableSnapshot snapshot) {
      NessieModelIceberg.setProperties(this, builder, snapshot);
    }
  }

  @NessieImmutable
  @JsonSerialize(as = ImmutableSetSnapshotRef.class)
  @JsonDeserialize(as = ImmutableSetSnapshotRef.class)
  interface SetSnapshotRef extends IcebergMetadataUpdate {
    String refName();

    Long snapshotId();

    String type(); // BRANCH or TAG

    @Nullable
    Integer minSnapshotsToKeep();

    @Nullable
    Long maxSnapshotAgeMs();

    @Nullable
    Long maxRefAgeMs();

    @Override
    default void apply(NessieTableSnapshot.Builder builder, NessieTableSnapshot snapshot) {
      // NOP - This class is used for JSON deserialization only.
      // Nessie has catalog-level branches and tags.
    }
  }
}
