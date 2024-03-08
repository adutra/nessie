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
package org.projectnessie.catalog.formats.iceberg.rest;

import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import java.util.Objects;
import java.util.UUID;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.nessie.immutables.NessieImmutable;

@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(
      value = IcebergUpdateRequirement.AssertTableUUID.class,
      name = "assert-table-uuid"),
  @JsonSubTypes.Type(
      value = IcebergUpdateRequirement.AssertViewUUID.class,
      name = "assert-view-uuid"),
  @JsonSubTypes.Type(
      value = IcebergUpdateRequirement.AssertTableDoesNotExist.class,
      name = "assert-create"),
  @JsonSubTypes.Type(
      value = IcebergUpdateRequirement.AssertRefSnapshotId.class,
      name = "assert-ref-snapshot-id"),
  @JsonSubTypes.Type(
      value = IcebergUpdateRequirement.AssertLastAssignedFieldId.class,
      name = "assert-last-assigned-field-id"),
  @JsonSubTypes.Type(
      value = IcebergUpdateRequirement.AssertCurrentSchemaId.class,
      name = "assert-current-schema-id"),
  @JsonSubTypes.Type(
      value = IcebergUpdateRequirement.AssertLastAssignedPartitionId.class,
      name = "assert-last-assigned-partition-id"),
  @JsonSubTypes.Type(
      value = IcebergUpdateRequirement.AssertDefaultSpecId.class,
      name = "assert-default-spec-id"),
  @JsonSubTypes.Type(
      value = IcebergUpdateRequirement.AssertDefaultSortOrderId.class,
      name = "assert-default-sort-order-id"),
})
public interface IcebergUpdateRequirement {

  void check(NessieTableSnapshot snapshot, boolean tableExists, String nessieRefName);

  @NessieImmutable
  @JsonTypeName("assert-table-uuid")
  @JsonSerialize(as = ImmutableAssertTableUUID.class)
  @JsonDeserialize(as = ImmutableAssertTableUUID.class)
  interface AssertTableUUID extends IcebergUpdateRequirement {
    String uuid();

    @Override
    default void check(NessieTableSnapshot snapshot, boolean tableExists, String nessieRefName) {
      UUID tableUuid = snapshot.entity().icebergUuid();
      String tableUuidString = tableUuid != null ? tableUuid.toString() : null;
      checkState(
          uuid().equals(tableUuidString),
          "Requirement failed: UUID does not match: expected %s != %s",
          tableUuidString,
          uuid());
    }
  }

  @NessieImmutable
  @JsonTypeName("assert-view-uuid")
  @JsonSerialize(as = ImmutableAssertViewUUID.class)
  @JsonDeserialize(as = ImmutableAssertViewUUID.class)
  interface AssertViewUUID extends IcebergUpdateRequirement {
    @Override
    default void check(NessieTableSnapshot snapshot, boolean tableExists, String nessieRefName) {
      throw new UnsupportedOperationException("view operations not supported on tables");
    }
  }

  @NessieImmutable
  @JsonTypeName("assert-create")
  @JsonSerialize(as = ImmutableAssertTableDoesNotExist.class)
  @JsonDeserialize(as = ImmutableAssertTableDoesNotExist.class)
  interface AssertTableDoesNotExist extends IcebergUpdateRequirement {
    @Override
    default void check(NessieTableSnapshot snapshot, boolean tableExists, String nessieRefName) {
      checkState(!tableExists, "Requirement failed: table already exists");
    }
  }

  @NessieImmutable
  @JsonTypeName("assert-ref-snapshot-id")
  @JsonSerialize(as = ImmutableAssertRefSnapshotId.class)
  @JsonDeserialize(as = ImmutableAssertRefSnapshotId.class)
  interface AssertRefSnapshotId extends IcebergUpdateRequirement {
    String ref();

    @Nullable
    Long snapshotId();

    @Override
    default void check(NessieTableSnapshot snapshot, boolean tableExists, String nessieRefName) {
      checkState(ref().equals(nessieRefName), "Expected Nessie reference name does not match");
      Long id = snapshotId();
      if (id != null) {
        checkState(
            Objects.equals(id, snapshot.icebergSnapshotId()),
            "Requirement failed: snapshot id changed: expected %s != %s",
            id,
            snapshotId());
      }
    }
  }

  @NessieImmutable
  @JsonTypeName("assert-last-assigned-field-id")
  @JsonSerialize(as = ImmutableAssertLastAssignedFieldId.class)
  @JsonDeserialize(as = ImmutableAssertLastAssignedFieldId.class)
  interface AssertLastAssignedFieldId extends IcebergUpdateRequirement {
    int lastAssignedFieldId();

    @Override
    default void check(NessieTableSnapshot snapshot, boolean tableExists, String nessieRefName) {
      Integer id = snapshot.icebergLastColumnId();
      checkState(
          lastAssignedFieldId() == id,
          "Requirement failed: last assigned field id changed: expected %s != %s",
          id,
          lastAssignedFieldId());
    }
  }

  @NessieImmutable
  @JsonTypeName("assert-current-schema-id")
  @JsonSerialize(as = ImmutableAssertCurrentSchemaId.class)
  @JsonDeserialize(as = ImmutableAssertCurrentSchemaId.class)
  interface AssertCurrentSchemaId extends IcebergUpdateRequirement {
    int currentSchemaId();

    @Override
    default void check(NessieTableSnapshot snapshot, boolean tableExists, String nessieRefName) {
      int id = snapshot.currentSchemaObject().icebergId();
      checkState(
          currentSchemaId() == id,
          "Requirement failed: current schema id changed: expected %s != %s",
          id,
          currentSchemaId());
    }
  }

  @NessieImmutable
  @JsonTypeName("assert-last-assigned-partition-id")
  @JsonSerialize(as = ImmutableAssertLastAssignedPartitionId.class)
  @JsonDeserialize(as = ImmutableAssertLastAssignedPartitionId.class)
  interface AssertLastAssignedPartitionId extends IcebergUpdateRequirement {
    int lastAssignedPartitionId();

    @Override
    default void check(NessieTableSnapshot snapshot, boolean tableExists, String nessieRefName) {
      Integer id = snapshot.icebergLastPartitionId();
      checkState(
          lastAssignedPartitionId() == id,
          "Requirement failed: last assigned partition id changed: expected %s != %s",
          id,
          lastAssignedPartitionId());
    }
  }

  @NessieImmutable
  @JsonTypeName("assert-default-spec-id")
  @JsonSerialize(as = ImmutableAssertDefaultSpecId.class)
  @JsonDeserialize(as = ImmutableAssertDefaultSpecId.class)
  interface AssertDefaultSpecId extends IcebergUpdateRequirement {
    int defaultSpecId();

    @Override
    default void check(NessieTableSnapshot snapshot, boolean tableExists, String nessieRefName) {
      int id = snapshot.currentPartitionDefinitionObject().icebergId();
      checkState(
          defaultSpecId() == id,
          "Requirement failed: default spec id changed: expected %s != %s",
          id,
          defaultSpecId());
    }
  }

  @NessieImmutable
  @JsonTypeName("assert-default-sort-order-id")
  @JsonSerialize(as = ImmutableAssertDefaultSortOrderId.class)
  @JsonDeserialize(as = ImmutableAssertDefaultSortOrderId.class)
  interface AssertDefaultSortOrderId extends IcebergUpdateRequirement {
    int defaultSortOrderId();

    @Override
    default void check(NessieTableSnapshot snapshot, boolean tableExists, String nessieRefName) {
      int id = snapshot.currentSortDefinitionObject().icebergSortOrderId();
      checkState(
          defaultSortOrderId() == id,
          "Requirement failed: default sort order id changed: expected %s != %s",
          id,
          defaultSortOrderId());
    }
  }
}
