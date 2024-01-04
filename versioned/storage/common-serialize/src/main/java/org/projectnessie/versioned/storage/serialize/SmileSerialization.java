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
package org.projectnessie.versioned.storage.serialize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.smile.databind.SmileMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import org.projectnessie.versioned.storage.common.json.ObjIdHelper;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.ObjTypes;
import org.projectnessie.versioned.storage.common.util.Compressions;

public final class SmileSerialization {

  private static final ObjectMapper SMILE_MAPPER = new SmileMapper().findAndRegisterModules();

  private SmileSerialization() {}

  public static Obj deserializeObj(ObjId id, byte[] data, String typeName, String compression) {
    ObjType type = ObjTypes.forName(typeName);
    return deserializeObj(id, data, type.targetClass(), compression);
  }

  public static Obj deserializeObj(ObjId id, ByteBuffer data, String typeName, String compression) {
    ObjType type = ObjTypes.forName(typeName);
    return deserializeObj(id, data, type.targetClass(), compression);
  }

  public static Obj deserializeObj(
      ObjId id, byte[] data, Class<? extends Obj> targetClass, String compression) {
    try {
      ObjectReader reader = ObjIdHelper.readerWithObjId(SMILE_MAPPER, targetClass, id);
      data = Compressions.uncompress(Compression.fromValue(compression), data);
      return reader.readValue(data, targetClass);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static Obj deserializeObj(
      ObjId id, ByteBuffer data, Class<? extends Obj> targetClass, String compression) {
    byte[] bytes = new byte[data.remaining()];
    data.get(bytes);
    return deserializeObj(id, bytes, targetClass, compression);
  }

  public static byte[] serializeObj(Obj obj, Consumer<Compression> compression) {
    try {
      return Compressions.compressDefault(SMILE_MAPPER.writeValueAsBytes(obj), compression);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }
}
