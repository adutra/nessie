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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.List;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.nessie.immutables.NessieImmutable;

/**
 * Persisted references to objects that are relevant for an entity's (table, view, etc) child
 * objects (schemas, partition definitions, etc).
 *
 * <p>A table's snapshot refers to a bunch of other objects/rows in the database:
 *
 * <ul>
 *   <li>snapshot details object
 *   <li>entity object (contains global, immutable/rarely changed attributes)
 *   <lI>relevant schemas (multiple)
 *   <lI>relevant partition definitions (multiple)
 *   <lI>relevant sort definitions (multiple)
 * </ul>
 *
 * <h2>(De)composing a Nessie Data Catalog model object, example using {@link
 * org.projectnessie.catalog.model.snapshot.NessieTableSnapshot}
 *
 * <p>A {@linkplain org.projectnessie.catalog.model.snapshot.NessieTableSnapshot table snapshot}
 * contains data and all relevant parts (schemas, partition-definitions, sort-definitions) of a
 * snapshot, even parts that are shared across multiple snapshots, and base information that
 * rarely/never changes, like the Nessie content ID or Iceberg's table-UUID.
 *
 * <p>The <em>persisted</em> value of a {@linkplain
 * org.projectnessie.catalog.model.snapshot.NessieTableSnapshot table snapshot} is split according
 * to the above list, separately persisted parts of the table snapshot are not persisted with the
 * table snapshot itself.
 *
 * <h2>Caching/stateful clients
 *
 * <p>Clients <em>may</em> can cache individual persisted objects, they indicate the IDs of the
 * objects they know about using a probabilistic data structure (e.g. bloom filter). The code that
 * request objects to be loaded via {@link CatalogStorage} handles the conditional loading for
 * clients.
 */
@NessieImmutable
public interface CatalogEntitySnapshot {
  /**
   * Technical ID of the snapshot.
   *
   * <p>The snapshot ID is derived from the contents of a Nessie {@link
   * org.projectnessie.model.Content} object, for example an {@link
   * org.projectnessie.model.IcebergTable} object.
   */
  NessieId snapshotId();

  NessieId entityId();

  NessieId currentSchema();

  /**
   * Contains the IDs of all schemas, except the {@linkplain #currentPartitionDefinition() ID of the
   * current one}.
   */
  List<NessieId> schemas();

  NessieId currentPartitionDefinition();

  /**
   * Contains the IDs of all partition definitions, except the {@linkplain
   * #currentPartitionDefinition() ID of the current one}.
   */
  List<NessieId> partitionDefinitions();

  NessieId currentSortDefinition();

  /**
   * Contains the IDs of all sort definitions, except the {@linkplain #currentPartitionDefinition()
   * ID of the current one}.
   */
  List<NessieId> sortDefinitions();

  List<NessieId> manifests();

  static Builder builder() {
    return ImmutableCatalogEntitySnapshot.builder();
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder from(CatalogEntitySnapshot instance);

    @CanIgnoreReturnValue
    Builder snapshotId(NessieId snapshotId);

    @CanIgnoreReturnValue
    Builder entityId(NessieId entityId);

    @CanIgnoreReturnValue
    Builder currentSchema(NessieId currentSchema);

    @CanIgnoreReturnValue
    Builder addSchemas(NessieId element);

    @CanIgnoreReturnValue
    Builder addSchemas(NessieId... elements);

    @CanIgnoreReturnValue
    Builder schemas(Iterable<? extends NessieId> elements);

    @CanIgnoreReturnValue
    Builder addAllSchemas(Iterable<? extends NessieId> elements);

    @CanIgnoreReturnValue
    Builder currentPartitionDefinition(NessieId currentPartitionDefinition);

    @CanIgnoreReturnValue
    Builder addPartitionDefinitions(NessieId element);

    @CanIgnoreReturnValue
    Builder addPartitionDefinitions(NessieId... elements);

    @CanIgnoreReturnValue
    Builder partitionDefinitions(Iterable<? extends NessieId> elements);

    @CanIgnoreReturnValue
    Builder addAllPartitionDefinitions(Iterable<? extends NessieId> elements);

    @CanIgnoreReturnValue
    Builder currentSortDefinition(NessieId currentSortDefinition);

    @CanIgnoreReturnValue
    Builder addSortDefinitions(NessieId element);

    @CanIgnoreReturnValue
    Builder addSortDefinitions(NessieId... elements);

    @CanIgnoreReturnValue
    Builder sortDefinitions(Iterable<? extends NessieId> elements);

    @CanIgnoreReturnValue
    Builder addAllSortDefinitions(Iterable<? extends NessieId> elements);

    @CanIgnoreReturnValue
    Builder addManifests(NessieId element);

    @CanIgnoreReturnValue
    Builder addManifests(NessieId... elements);

    @CanIgnoreReturnValue
    Builder manifests(Iterable<? extends NessieId> elements);

    @CanIgnoreReturnValue
    Builder addAllManifests(Iterable<? extends NessieId> elements);

    CatalogEntitySnapshot build();
  }
}
