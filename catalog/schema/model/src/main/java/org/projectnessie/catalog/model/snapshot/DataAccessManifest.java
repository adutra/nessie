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
import java.util.List;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableDataAccessManifest.class)
@JsonDeserialize(as = ImmutableDataAccessManifest.class)
public interface DataAccessManifest {
  NessieId dataAccessManifestId();

  List<DataFileManifest> dataFiles();

  // TODO encryption information

  NessieId partitionDefinitionId();

  /**
   * If present and non-empty, contains information whether a partition-column in the {@link
   * #dataFiles() referenced data files} contains {@code null}.
   *
   * <p>List elements correspond to the {@link NessiePartitionDefinition#columns() partition
   * definition}. The number of elements for this value might be lower than the total number of
   * partition columns.
   */
  // Supported by: Iceberg
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  // TODO this should be a BitSet
  List<Boolean> perPartitionColumnContainsNull();

  /**
   * If present and non-empty, contains information whether a partition-column in the {@link
   * #dataFiles() referenced data files} contains NaN.
   *
   * <p>List elements correspond to the {@link NessiePartitionDefinition#columns() partition
   * definition}. The number of elements for this value might be lower than the total number of
   * partition columns.
   */
  // Supported by: Iceberg
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  // TODO this should be a BitSet
  List<Boolean> perPartitionColumnContainsNan();

  /**
   * If present and non-empty, contains information about the lower-bound of the partition-columns
   * in the {@link #dataFiles() referenced data files}.
   *
   * <p>List elements correspond to the {@link NessiePartitionDefinition#columns() partition
   * definition}. The number of elements for this value might be lower than the total number of
   * partition columns.
   */
  // Supported by: Iceberg
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<FieldValue> perPartitionColumnLowerBound();

  /**
   * If present and non-empty, contains information about the upper-bound of the partition-columns
   * in the {@link #dataFiles() referenced data files}.
   *
   * <p>ist elements correspond to the {@link NessiePartitionDefinition#columns() partition
   * definition}. The number of elements for this value might be lower than the total number of
   * partition columns.
   */
  // Supported by: Iceberg
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<FieldValue> perPartitionColumnUpperBound();
}
