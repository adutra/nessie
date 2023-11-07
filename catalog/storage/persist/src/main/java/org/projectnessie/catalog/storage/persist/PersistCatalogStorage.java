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
package org.projectnessie.catalog.storage.persist;

import static java.util.Collections.emptyList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.id.NessieIdHasher;
import org.projectnessie.catalog.storage.backend.CatalogEntitySnapshot;
import org.projectnessie.catalog.storage.backend.CatalogStorage;
import org.projectnessie.catalog.storage.backend.ObjectMismatchException;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.objtypes.StringObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;

public class PersistCatalogStorage implements CatalogStorage {

  private final Persist persist;

  public PersistCatalogStorage(Persist persist) {
    this.persist = persist;
  }

  @Override
  public CatalogEntitySnapshot loadSnapshot(NessieId snapshotId) {
    NessieId storeId =
        NessieIdHasher.nessieIdHasher().hash("CatalogEntitySnapshot").hash(snapshotId).generate();
    return loadObject(storeId, CatalogEntitySnapshot.class);
  }

  @Override
  public void createSnapshot(CatalogEntitySnapshot snapshot) throws ObjectMismatchException {
    NessieId storeId =
        NessieIdHasher.nessieIdHasher()
            .hash("CatalogEntitySnapshot")
            .hash(snapshot.snapshotId())
            .generate();
    createObject(storeId, snapshot);
  }

  public <T> T loadObject(NessieId id, Class<T> expectedType) {
    try {
      Obj obj = persist.fetchObj(ObjId.objIdFromByteArray(id.idAsBytes()));
      return deserialize(obj, expectedType);
    } catch (ObjNotFoundException e) {
      return null;
    }
  }

  @Override
  public void loadObjects(
      Collection<NessieId> ids, BiConsumer<NessieId, Object> loaded, Consumer<NessieId> notFound) {
    NessieId[] nessieIds = ids.stream().distinct().toArray(NessieId[]::new);
    ObjId[] objIds =
        Arrays.stream(nessieIds)
            .map(NessieId::idAsBytes)
            .map(ObjId::objIdFromByteArray)
            .toArray(ObjId[]::new);
    try {
      Obj[] objs = persist.fetchObjs(objIds);
      for (int i = 0; i < objs.length; i++) {
        Obj obj = objs[i];
        if (obj == null) {
          notFound.accept(nessieIds[i]);
        } else {
          loaded.accept(nessieIds[i], deserialize(obj, Object.class));
        }
      }
    } catch (ObjNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void createObject(NessieId id, Object object) throws ObjectMismatchException {
    try {
      ObjId objId = ObjId.objIdFromByteArray(id.idAsBytes());
      Obj obj = serialize(objId, object);
      boolean stored = persist.storeObj(obj);
      if (!stored) {
        Obj previousObj = persist.fetchObj(objId);
        if (!Objects.equals(previousObj, obj)) {
          throw new ObjectMismatchException(id);
        }
      }
    } catch (ObjTooLargeException | ObjNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void createObjects(Map<NessieId, Object> objects) throws ObjectMismatchException {
    try {
      Obj[] objs =
          objects.entrySet().stream()
              .map(e -> serialize(ObjId.objIdFromByteArray(e.getKey().idAsBytes()), e.getValue()))
              .toArray(Obj[]::new);
      boolean[] stored = persist.storeObjs(objs);
      Obj[] existingObjs =
          IntStream.range(0, objs.length)
              .filter(i -> !stored[i])
              .mapToObj(i -> objs[i])
              .toArray(Obj[]::new);
      if (existingObjs.length > 0) {
        ObjId[] existingIds = Arrays.stream(existingObjs).map(Obj::id).toArray(ObjId[]::new);
        Obj[] previousObjs = persist.fetchObjs(existingIds);
        for (int i = 0; i < previousObjs.length; i++) {
          if (!Objects.equals(previousObjs[i], existingObjs[i])) {
            NessieId nessieId = NessieId.nessieIdFromBytes(existingIds[i].asByteArray());
            throw new ObjectMismatchException(nessieId);
          }
        }
      }
    } catch (ObjTooLargeException | ObjNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().registerModule(new GuavaModule());

  private Obj serialize(ObjId id, Object value) {
    try {
      return StringObj.stringData(
          id,
          "application/json",
          Compression.NONE,
          // FIXME use the interface type instead of the implementation type,
          // or use constant discriminators
          value.getClass().getName(),
          emptyList(),
          ByteString.copyFrom(OBJECT_MAPPER.writeValueAsBytes(value)));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private <T> T deserialize(Obj obj, Class<T> expectedType) {
    if (!(obj instanceof StringObj)) {
      throw new UnsupportedOperationException("Unknown object type " + obj.type());
    }
    if (!((StringObj) obj).contentType().equals("application/json")) {
      throw new UnsupportedOperationException(
          "Unknown content type " + ((StringObj) obj).contentType());
    }
    String className = ((StringObj) obj).filename();
    ByteString payload = ((StringObj) obj).text();
    try {
      @SuppressWarnings("unchecked")
      Class<T> clazz = (Class<T>) Class.forName(className);
      if (!expectedType.isAssignableFrom(clazz)) {
        throw new UnsupportedOperationException(
            "Unexpected JSON payload type "
                + clazz.getName()
                + ", expected instance of "
                + expectedType.getName());
      }
      return OBJECT_MAPPER.readValue(payload.toByteArray(), clazz);
    } catch (ClassNotFoundException | IOException e) {
      throw new RuntimeException(e);
    }
  }
}
