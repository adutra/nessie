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
package org.projectnessie.catalog.formats.iceberg.nessie;

import static java.util.Collections.emptyMap;

import java.util.List;
import java.util.Map;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;

/**
 * Maintains state when applying {@linkplain IcebergMetadataUpdate Iceberg metadata updates} to a
 * {@linkplain NessieTableSnapshot table snapshot}.
 *
 * <p>State includes:
 *
 * <ul>
 *   <li>Last {@linkplain IcebergMetadataUpdate.AddSchema added schema} id for {@link
 *       IcebergMetadataUpdate.SetCurrentSchema SetCurrentSchema}
 *   <li>Last {@linkplain IcebergMetadataUpdate.AddPartitionSpec added partition spec} id for {@link
 *       IcebergMetadataUpdate.SetDefaultPartitionSpec SetDefaultPartitionSpec}
 *   <li>Last {@linkplain IcebergMetadataUpdate.AddSortOrder added sort order} id for {@link
 *       IcebergMetadataUpdate.SetDefaultSortOrder SetDefaultSortOrder}
 * </ul>
 */
public class IcebergMetadataUpdateState {
  private final NessieTableSnapshot.Builder builder;

  private int lastAddedSchemaId = -1;
  private int lastAddedSpecId = -1;
  private int lastAddedOrderId = -1;

  public IcebergMetadataUpdateState(NessieTableSnapshot snapshot) {
    this.builder = NessieTableSnapshot.builder().from(snapshot);
  }

  public NessieTableSnapshot.Builder builder() {
    return builder;
  }

  public NessieTableSnapshot snapshot() {
    return builder.build();
  }

  public int lastAddedSchemaId() {
    return lastAddedSchemaId;
  }

  public void lastAddedSchemaId(int lastAddedSchemaId) {
    this.lastAddedSchemaId = lastAddedSchemaId;
  }

  public int lastAddedSpecId() {
    return lastAddedSpecId;
  }

  public void lastAddedSpecId(int lastAddedSpecId) {
    this.lastAddedSpecId = lastAddedSpecId;
  }

  public int lastAddedOrderId() {
    return lastAddedOrderId;
  }

  public void lastAddedOrderId(int lastAddedOrderId) {
    this.lastAddedOrderId = lastAddedOrderId;
  }

  public IcebergMetadataUpdateState applyUpdates(List<IcebergMetadataUpdate> updates) {
    for (IcebergMetadataUpdate update : updates) {
      update.apply(this);
    }
    return this;
  }

  private Map<Integer, Integer> remappedFieldIds;

  public void remappedFields(Map<Integer, Integer> remappedFieldIds) {
    this.remappedFieldIds = remappedFieldIds;
  }

  public int mapFieldId(int sourceId, NessieId schemaId) {
    Map<Integer, Integer> map = remappedFieldIds;
    if (map == null) {
      map = emptyMap();
    }
    System.err.println("TRY MAP FIELD " + sourceId + " " + schemaId + " --> " + map.get(sourceId));
    return map.getOrDefault(sourceId, sourceId);
  }
}
