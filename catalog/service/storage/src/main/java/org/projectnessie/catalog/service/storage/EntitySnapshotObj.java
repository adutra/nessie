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
package org.projectnessie.catalog.service.storage;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.projectnessie.versioned.storage.common.objtypes.CustomObjType.dynamicCaching;
import static org.projectnessie.versioned.storage.common.persist.ObjType.CACHE_UNLIMITED;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nullable;
import java.time.Instant;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.versioned.storage.common.objtypes.CustomObjType;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

@NessieImmutable
@JsonSerialize(as = ImmutableEntitySnapshotObj.class)
@JsonDeserialize(as = ImmutableEntitySnapshotObj.class)
public interface EntitySnapshotObj extends Obj {

  @Override
  ObjId id();

  @Override
  @Value.NonAttribute
  default ObjType type() {
    return OBJ_TYPE;
  }

  /** Snapshot value. */
  @Nullable
  NessieEntitySnapshot<?> snapshot();

  /** ID of the entity-object. */
  @Nullable
  ObjId entity();

  /** ID of the manifest-group (Iceberg manifest-list). */
  @Nullable
  ObjId manifestGroup();

  @Nullable
  Instant refreshAt();

  @Nullable
  String failure();

  CustomObjType.CacheExpireCalculation<EntitySnapshotObj> CACHE_EXPIRE =
      (obj, currentTimeMicros) -> {
        if (obj.failure() != null) {
          return CACHE_UNLIMITED;
        }

        Instant refreshAt = obj.refreshAt();
        if (refreshAt != null) {
          return MILLISECONDS.toMicros(refreshAt.toEpochMilli());
        }

        return CACHE_UNLIMITED;
      };

  ObjType OBJ_TYPE =
      dynamicCaching("catalog-snapshot", "c-s", EntitySnapshotObj.class, CACHE_EXPIRE);

  static Builder builder() {
    return ImmutableEntitySnapshotObj.builder();
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder id(ObjId id);

    @CanIgnoreReturnValue
    Builder snapshot(NessieEntitySnapshot<?> snapshot);

    @CanIgnoreReturnValue
    Builder entity(ObjId entity);

    @CanIgnoreReturnValue
    Builder manifestGroup(ObjId manifestGroup);

    @CanIgnoreReturnValue
    Builder refreshAt(Instant refreshAt);

    @CanIgnoreReturnValue
    Builder failure(String failure);

    EntitySnapshotObj build();
  }
}
