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
package org.projectnessie.catalog.model.id;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Represents an ID in the Nessie Catalog. Users of this class must not interpret the value/content
 * of the ID. Nessie Catalog IDs consist only of bytes.
 */
@JsonSerialize(using = NessieIdSerializer.class)
@JsonDeserialize(using = NessieIdDeserializer.class)
public interface NessieId extends Hashable {

  static NessieId randomNessieId() {
    byte[] id = new byte[32];
    ThreadLocalRandom rand = ThreadLocalRandom.current();
    for (int i = 0; i < id.length; i++) {
      id[i] = (byte) rand.nextInt();
    }
    // TODO add specialized NessieId implementations
    return NessieIdGeneric.nessieIdFromBytes(id);
  }

  static NessieId nessieIdFromUUID(UUID id) {
    // TODO add specialized NessieId implementations
    return NessieIdGeneric.nessieIdFromUUID(id);
  }

  static NessieId nessieIdFromString(String id) {
    // TODO add specialized NessieId implementations
    return NessieIdGeneric.nessieIdFromString(id);
  }

  static NessieId nessieIdFromStringBase64(String id) {
    // TODO add specialized NessieId implementations
    return NessieIdGeneric.nessieIdFromStringBase64(id);
  }

  static NessieId nessieIdFromBytes(byte[] bytes) {
    // TODO add specialized NessieId implementations
    // TODO safeguard: bytes.clone() or the like
    return NessieIdGeneric.nessieIdFromBytes(bytes);
  }

  static NessieId emptyNessieId() {
    // TODO add specialized NessieId implementations
    return NessieIdGeneric.emptyNessieId();
  }

  /** Constructs a new ID instance starting with the value of this ID plus the given UUID. */
  NessieId compositeWithUUID(UUID id);

  /**
   * Extracts the last 16 bytes as a {@link UUID}, assuming this ID instance was created via {@link
   * #compositeWithUUID(UUID)}.
   */
  UUID uuidFromComposite();

  ByteBuffer id();

  String idAsString();

  String idAsBase64();

  byte[] idAsBytes();

  /**
   * Builds the string representation, assuming this instance was created via {@link
   * #compositeWithUUID(UUID)}.
   */
  String idAsStringWithUUID();

  @Override
  default void hash(NessieIdHasher idHasher) {
    idHasher.hash(id());
  }
}
