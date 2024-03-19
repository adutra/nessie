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

import static java.time.Instant.now;
import static java.util.Collections.emptyMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshot;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateRequirement;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.snapshot.NessieViewSnapshot;
import org.projectnessie.model.ContentKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains state when applying {@linkplain IcebergMetadataUpdate Iceberg metadata updates} to a
 * {@linkplain org.projectnessie.catalog.model.snapshot.NessieViewSnapshot view snapshot}.
 *
 * <p>State includes:
 *
 * <ul>
 *   <li>Last {@linkplain IcebergMetadataUpdate.AddSchema added schema} id for {@link
 *       IcebergMetadataUpdate.SetCurrentSchema SetCurrentSchema}
 * </ul>
 */
public class IcebergViewMetadataUpdateState {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IcebergViewMetadataUpdateState.class);

  private final NessieViewSnapshot.Builder builder;
  private final ContentKey key;
  private final boolean viewExists;

  private NessieViewSnapshot snapshot;
  private int lastAddedSchemaId = -1;
  private long lastAddedVersionId = -1;
  private final List<IcebergSnapshot> addedSnapshots = new ArrayList<>();
  private final Set<Integer> addedSchemaIds = new HashSet<>();
  private final Set<Long> addedVersionIds = new HashSet<>();

  public IcebergViewMetadataUpdateState(
      NessieViewSnapshot snapshot, ContentKey key, boolean viewExists) {
    this.snapshot = snapshot;
    this.builder = NessieViewSnapshot.builder().from(snapshot);
    this.key = key;
    this.viewExists = viewExists;
  }

  public NessieViewSnapshot.Builder builder() {
    return builder;
  }

  public NessieViewSnapshot snapshot() {
    return snapshot;
  }

  public int lastAddedSchemaId() {
    return lastAddedSchemaId;
  }

  public void schemaAdded(int schemaId) {
    // TODO reduce log level to trace (or remove logging)
    LOGGER.info("added schema ID {}", schemaId);
    if (schemaId >= 0) {
      addedSchemaIds.add(schemaId);
    }
    lastAddedSchemaId = schemaId;
  }

  public boolean isAddedSchema(int schemaId) {
    return addedSchemaIds.contains(schemaId);
  }

  public long lastAddedVersionId() {
    return lastAddedVersionId;
  }

  public void versionAdded(long versionId) {
    // TODO reduce log level to trace (or remove logging)
    LOGGER.info("added version ID {}", versionId);
    if (versionId >= 0) {
      addedVersionIds.add(versionId);
    }
    lastAddedVersionId = versionId;
  }

  public boolean isAddedVersion(long versionId) {
    return addedVersionIds.contains(versionId);
  }

  public IcebergViewMetadataUpdateState checkRequirements(
      List<IcebergUpdateRequirement> requirements) {
    // TODO reduce log level to trace
    LOGGER.info("check {} requirements against {}", requirements.size(), key);
    for (IcebergUpdateRequirement requirement : requirements) {
      LOGGER.info("check requirement: {}", requirement);
      requirement.checkForView(snapshot, viewExists, key);
    }
    return this;
  }

  public IcebergViewMetadataUpdateState applyUpdates(List<IcebergMetadataUpdate> updates) {
    // TODO reduce log level to trace
    LOGGER.info("apply {} updates to {}", updates.size(), key);
    for (IcebergMetadataUpdate update : updates) {
      LOGGER.info("apply update: {}", update);
      update.applyToView(this);
      snapshot = builder.lastUpdatedTimestamp(now()).build();
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
    return map.getOrDefault(sourceId, sourceId);
  }
}
