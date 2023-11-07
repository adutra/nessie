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
package org.projectnessie.catalog.storage.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.projectnessie.catalog.model.id.NessieId;

public interface CatalogStorage {

  // TODO Might need conditional update functionality - e.g. to conditionally bump the
  //  `NessieTable.icebergLastColumnId` or `NessieTable.icebergLastPartitionId` values to coordinate
  //  schema changes across multiple Nessie references.

  CatalogEntitySnapshot loadSnapshot(NessieId id);

  /**
   * Persist a new snapshot.
   *
   * <p>If the snapshot did not already exist, it will be created in the database.
   *
   * <p>If the snapshot already exists, the currently persisted value will be compared against the
   * given snapshot object. An exception will be thrown when the persisted and given objects are not
   * equal.
   */
  void createSnapshot(CatalogEntitySnapshot snapshot) throws ObjectMismatchException;

  void loadObjects(
      Collection<NessieId> ids, BiConsumer<NessieId, Object> loaded, Consumer<NessieId> notFound);

  // FIXME unused
  default Collection<Object> loadObjects(Collection<NessieId> ids, Consumer<NessieId> notFound) {
    Collection<Object> result = new ArrayList<>();
    loadObjects(ids, (id, obj) -> result.add(obj), notFound);
    return result;
  }

  default Object loadObject(NessieId id) {
    return loadObject(id, Object.class);
  }

  default <T> T loadObject(NessieId id, Class<T> expectedType) {
    return loadObjects(Collections.singleton(id), i -> {}).stream()
        .filter(o -> expectedType.isAssignableFrom(o.getClass()))
        .map(expectedType::cast)
        .findFirst()
        .orElse(null);
  }

  /**
   * Persist new objects.
   *
   * <p>If an object did not already exist, it will be created in the database.
   *
   * <p>If an object already exists, the currently persisted value will be compared against the
   * given object. An exception will be thrown when the persisted and given objects are not equal.
   */
  void createObjects(Map<NessieId, Object> objects) throws ObjectMismatchException;

  default void createObject(NessieId id, Object object) throws ObjectMismatchException {
    createObjects(Collections.singletonMap(id, object));
  }
}
