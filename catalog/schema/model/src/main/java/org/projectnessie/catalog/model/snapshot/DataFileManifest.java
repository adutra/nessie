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
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.model.CommitMeta.InstantDeserializer;
import org.projectnessie.model.CommitMeta.InstantSerializer;
import org.projectnessie.nessie.immutables.NessieImmutable;

/** Describes a single data file. */
@NessieImmutable
@JsonSerialize(as = ImmutableDataFileManifest.class)
@JsonDeserialize(as = ImmutableDataFileManifest.class)
public interface DataFileManifest {
  NessieId dataFileId();

  NessieId schemaId();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  PartitionIdentifier partitionIdentifier();

  NessieId baseLocationId();

  URI relativeUri();

  DataFileType fileType();

  long fileSize();

  // Supported by: Iceberg
  NessieId sortDefinitionId();

  // Supported by: Delta
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  Instant modificationTime();

  // Supported by: Delta
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Map<String, String> tags();

  // Supported by: Delta
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Boolean dataChange();

  // Supported by: Delta
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Long baseRowId();

  // TODO Delta "deletion vector"

  // Supported by: Delta
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Long defaultRowCommitVersion();

  // Supported by: Delta + Iceberg
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Long recordCount();

  // Supported by: Iceberg
  List<Long> splittableOffsets();

  // Supported by: Iceberg
  List<NessieId> equalityFieldIds();

  // TODO encryption information

  // Note: using lists for each value type, with the index corresponding to the columns in the
  // referenced schema to allow more efficient serialization / compression.

  /**
   * Contains indexes to the columns corresponding to the referenced {@link NessieSchema}, if the
   * indexes in the per-column lists differ from the referenced schema's columns.
   */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<Integer> perColumnIds();

  /**
   * Data size per column.
   *
   * <p>The indexes correspond to the indexes of {@link #perColumnIds()} or, if {@link
   * #perColumnIds()} is empty, correspond to the indexes of the referenced {@link NessieSchema}.
   * The number of elements for this value might be lower than the total number of columns.
   */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<Long> perColumnDataSize();

  /**
   * Total value count per column.
   *
   * <p>The indexes correspond to the indexes of {@link #perColumnIds()} or, if {@link
   * #perColumnIds()} is empty, correspond to the indexes of the referenced {@link NessieSchema}.
   * The number of elements for this value might be lower than the total number of columns.
   */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<Long> perColumnTotalValueCount();

  /**
   * Count of {@code null} values per column.
   *
   * <p>The indexes correspond to the indexes of {@link #perColumnIds()} or, if {@link
   * #perColumnIds()} is empty, correspond to the indexes of the referenced {@link NessieSchema}.
   * The number of elements for this value might be lower than the total number of columns.
   */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<Long> perColumnNullValueCount();

  /**
   * Count of NaN values per column.
   *
   * <p>The indexes correspond to the indexes of {@link #perColumnIds()} or, if {@link
   * #perColumnIds()} is empty, correspond to the indexes of the referenced {@link NessieSchema}.
   * The number of elements for this value might be lower than the total number of columns.
   */
  // Supported by: Iceberg
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<Long> perColumnNanValueCount();

  /**
   * Lower bounds per column.
   *
   * <p>The indexes correspond to the indexes of {@link #perColumnIds()} or, if {@link
   * #perColumnIds()} is empty, correspond to the indexes of the referenced {@link NessieSchema}.
   * The number of elements for this value might be lower than the total number of columns.
   */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<FieldValue> perColumnLowerBound();

  /**
   * Upper bounds per column.
   *
   * <p>The indexes correspond to the indexes of {@link #perColumnIds()} or, if {@link
   * #perColumnIds()} is empty, correspond to the indexes of the referenced {@link NessieSchema}.
   * The number of elements for this value might be lower than the total number of columns.
   */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<FieldValue> perColumnUpperBound();

  /**
   * Sum per column.
   *
   * <p>The indexes correspond to the indexes of {@link #perColumnIds()} or, if {@link
   * #perColumnIds()} is empty, correspond to the indexes of the referenced {@link NessieSchema}.
   * The number of elements for this value might be lower than the total number of columns.
   */
  // NOT provided by either Iceberg or Delta, but present in ORC files
  // TODO is this statistical information useful?
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<FieldValue> perColumSum();
}
