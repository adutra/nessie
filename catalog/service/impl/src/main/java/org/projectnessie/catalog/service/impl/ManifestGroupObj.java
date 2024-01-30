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
package org.projectnessie.catalog.service.impl;

import static org.projectnessie.versioned.storage.common.objtypes.CustomObjType.dynamicCaching;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.manifest.NessieFileManifestGroup;
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.nessie.tasks.api.TaskObj;
import org.projectnessie.nessie.tasks.api.TaskState;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

@NessieImmutable
@JsonSerialize(as = ImmutableManifestGroupObj.class)
@JsonDeserialize(as = ImmutableManifestGroupObj.class)
// Suppress: "Constructor parameters should be better defined on the same level of inheritance
// hierarchy..."
@SuppressWarnings("immutables:subtype")
public interface ManifestGroupObj extends TaskObj {

  @Override
  @Value.Default
  default ObjType type() {
    return OBJ_TYPE;
  }

  @Nullable
  NessieFileManifestGroup manifestGroup();

  ObjType OBJ_TYPE =
      dynamicCaching(
          "catalog-mgroup", "c-mg", ManifestGroupObj.class, TaskObj.taskDefaultCacheExpire());

  static Builder builder() {
    return ImmutableManifestGroupObj.builder();
  }

  interface Builder extends TaskObj.Builder {
    @CanIgnoreReturnValue
    Builder from(ManifestGroupObj obj);

    @CanIgnoreReturnValue
    Builder id(ObjId id);

    @CanIgnoreReturnValue
    Builder manifestGroup(NessieFileManifestGroup manifestGroup);

    @CanIgnoreReturnValue
    Builder taskState(TaskState taskState);

    ManifestGroupObj build();
  }
}
