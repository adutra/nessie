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

import java.nio.ByteBuffer;
import java.util.UUID;
import org.projectnessie.catalog.model.id.NessieId;

/**
 * A form of {@link NessieId} that does not contain any data and cannot be compared of serialized.
 * This {@link NessieId} implementation should be used only in transient objects.
 */
public class NessieIdTransient implements NessieId {
  private static final NessieId INSTANCE = new NessieIdTransient();

  public static NessieId instance() {
    return INSTANCE;
  }

  @Override
  public NessieId compositeWithUUID(UUID id) {
    throw new UnsupportedOperationException("Transient ID");
  }

  @Override
  public UUID uuidFromComposite() {
    throw new UnsupportedOperationException("Transient ID");
  }

  @Override
  public ByteBuffer id() {
    throw new UnsupportedOperationException("Transient ID");
  }

  @Override
  public String idAsString() {
    throw new UnsupportedOperationException("Transient ID");
  }

  @Override
  public String idAsBase64() {
    throw new UnsupportedOperationException("Transient ID");
  }

  @Override
  public byte[] idAsBytes() {
    throw new UnsupportedOperationException("Transient ID");
  }

  @Override
  public String idAsStringWithUUID() {
    throw new UnsupportedOperationException("Transient ID");
  }
}
