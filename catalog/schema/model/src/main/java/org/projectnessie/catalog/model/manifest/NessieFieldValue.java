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
package org.projectnessie.catalog.model.manifest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public interface NessieFieldValue {
  NessieId fieldId();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Integer icebergFieldId();

  ByteBuffer value();

  static Builder builder() {
    return ImmutableNessieFieldValue.builder();
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder from(NessieFieldValue instance);

    @CanIgnoreReturnValue
    Builder fieldId(NessieId fieldId);

    @CanIgnoreReturnValue
    Builder icebergFieldId(@Nullable Integer icebergFieldId);

    @CanIgnoreReturnValue
    Builder value(ByteBuffer value);

    NessieFieldValue build();
  }
}
