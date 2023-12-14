/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.gc.contents;

import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import org.immutables.value.Value;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;

/**
 * Content references provide the relevant information for live-content identification plus expiry
 * resp. orphan files deletion.
 */
@Value.Immutable
public interface ContentReference {

  /** Value from {@link Content#getId()}. */
  @Value.Parameter(order = 1)
  String contentId();

  /**
   * Nessie commit ID from which the referenced content can be retrieved via {@link #contentKey()}.
   */
  @Value.Auxiliary
  @Value.Parameter(order = 2)
  String commitId();

  /**
   * Content key in Nessie {@link #commitId() commit} from which the referenced content can be
   * retrieved.
   */
  @Value.Auxiliary
  @Value.Parameter(order = 3)
  ContentKey contentKey();

  @Value.Parameter(order = 4)
  Content.Type contentType();

  /** Value from {@link IcebergTable#getMetadataLocation()}. */
  @Value.Parameter(order = 5)
  @Nullable
  String metadataLocation();

  /** Value from {@link IcebergTable#getSnapshotId()}. */
  @Value.Parameter(order = 6)
  @Nullable
  Long snapshotId();

  static ContentReference icebergTable(
      @NotNull String contentId,
      @NotNull String commitId,
      @NotNull ContentKey contentKey,
      @NotNull String metadataLocation,
      long snapshotId) {
    return ImmutableContentReference.of(
        contentId, commitId, contentKey, Content.Type.ICEBERG_TABLE, metadataLocation, snapshotId);
  }
}
