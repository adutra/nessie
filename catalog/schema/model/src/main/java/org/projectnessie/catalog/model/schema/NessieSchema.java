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
package org.projectnessie.catalog.model.schema;

import static org.projectnessie.catalog.model.id.NessieIdHasher.nessieIdHasher;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.UUID;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableNessieSchema.class)
@JsonDeserialize(as = ImmutableNessieSchema.class)
public interface NessieSchema {

  int NO_SCHEMA_ID = -1;

  NessieId id();

  NessieStruct struct();

  @Value.Default
  default int icebergId() {
    return NO_SCHEMA_ID;
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<UUID> identifierFields();

  static NessieSchema nessieSchema(
      NessieStruct struct, int icebergSchemaId, List<UUID> identifierFieldIds) {
    return nessieSchema(
        nessieIdHasher("NessieSchema")
            .hash(struct)
            .hash(icebergSchemaId)
            .hashUuidCollection(identifierFieldIds)
            .generate(),
        struct,
        icebergSchemaId,
        identifierFieldIds);
  }

  static NessieSchema nessieSchema(
      NessieId schemaId, NessieStruct struct, int icebergSchemaId, List<UUID> identifierFieldIds) {
    return ImmutableNessieSchema.of(schemaId, struct, icebergSchemaId, identifierFieldIds);
  }
}
