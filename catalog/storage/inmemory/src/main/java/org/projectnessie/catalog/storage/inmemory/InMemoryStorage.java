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
package org.projectnessie.catalog.storage.inmemory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.storage.backend.CatalogEntitySnapshot;
import org.projectnessie.catalog.storage.backend.CatalogStorage;
import org.projectnessie.catalog.storage.backend.ObjectMismatchException;

public class InMemoryStorage implements CatalogStorage {
  private final Map<NessieId, CatalogEntitySnapshot> snapshots = new ConcurrentHashMap<>();
  private final Map<NessieId, Object> objects = new ConcurrentHashMap<>();

  @Override
  public CatalogEntitySnapshot loadSnapshot(NessieId id) {
    return snapshots.get(id);
  }

  @Override
  public void createSnapshot(CatalogEntitySnapshot snapshot) throws ObjectMismatchException {
    CatalogEntitySnapshot existing = snapshots.putIfAbsent(snapshot.snapshotId(), snapshot);
    if (existing != null && !existing.equals(snapshot)) {
      throw new ObjectMismatchException(snapshot.snapshotId());
    }
  }

  @Override
  public void loadObjects(
      Collection<NessieId> ids, BiConsumer<NessieId, Object> loaded, Consumer<NessieId> notFound) {
    for (NessieId id : ids) {
      Object obj = objects.get(id);
      if (obj != null) {
        loaded.accept(id, obj);
      } else {
        notFound.accept(id);
      }
    }
  }

  @Override
  public Object loadObject(NessieId id) {
    return objects.get(id);
  }

  @Override
  public Collection<Object> loadObjects(Collection<NessieId> ids, Consumer<NessieId> notFound) {
    Collection<Object> result = new ArrayList<>();
    loadObjects(ids, (id, obj) -> result.add(obj), notFound);
    return result;
  }

  @Override
  public void createObjects(Map<NessieId, Object> objects) throws ObjectMismatchException {
    for (Map.Entry<NessieId, Object> e : objects.entrySet()) {
      NessieId id = e.getKey();
      Object obj = e.getValue();
      Object existing = this.objects.putIfAbsent(id, obj);
      if (existing != null && !existing.equals(obj)) {
        throw new ObjectMismatchException(id);
      }
    }
  }
}
