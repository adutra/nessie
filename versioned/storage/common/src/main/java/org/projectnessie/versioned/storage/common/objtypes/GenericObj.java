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
package org.projectnessie.versioned.storage.common.objtypes;

import javax.annotation.Nonnull;
import org.immutables.value.Value;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

@Value.Immutable
public interface GenericObj extends Obj {

  @Override
  default ObjType type() {
    return ObjType.GENERIC;
  }

  @Override
  @Value.Parameter(order = 1)
  @Nonnull
  @jakarta.annotation.Nonnull
  ObjId id();

  @Value.Parameter(order = 2)
  @Nonnull
  @jakarta.annotation.Nonnull
  String contentType();

  @Value.Parameter(order = 3)
  @Nonnull
  @jakarta.annotation.Nonnull
  ByteString payload();

  @Nonnull
  @jakarta.annotation.Nonnull
  static GenericObj genericData(
      @Nonnull @jakarta.annotation.Nonnull ObjId id,
      @Nonnull @jakarta.annotation.Nonnull String contentType,
      @Nonnull @jakarta.annotation.Nonnull ByteString payload) {
    return ImmutableGenericObj.of(id, contentType, payload);
  }
}
