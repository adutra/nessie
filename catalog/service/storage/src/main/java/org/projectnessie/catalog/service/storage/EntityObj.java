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

import static org.projectnessie.versioned.storage.common.objtypes.CustomObjType.customObjType;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.NessieEntity;
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

@NessieImmutable
@JsonSerialize(as = ImmutableEntityObj.class)
@JsonDeserialize(as = ImmutableEntityObj.class)
public interface EntityObj extends Obj {

  @Override
  ObjId id();

  @Override
  @Value.NonAttribute
  default ObjType type() {
    return OBJ_TYPE;
  }

  NessieEntity entity();

  ObjType OBJ_TYPE = customObjType("catalog-entity", "c-e", EntityObj.class);

  static Builder builder() {
    return ImmutableEntityObj.builder();
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder id(ObjId id);

    @CanIgnoreReturnValue
    Builder entity(NessieEntity entity);

    EntityObj build();
  }
}
