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
package org.projectnessie.catalog.api.base.transport;

import java.util.Map;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.NessieView;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.locations.BaseLocation;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.catalog.model.schema.NessieSortDefinition;
import org.projectnessie.catalog.model.snapshot.DataAccessManifest;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.NessieViewSnapshot;

public interface ResultObjects {
  Map<NessieId, NessieTable> tables();

  Map<NessieId, NessieView> views();

  Map<NessieId, NessieSchema> schemas();

  Map<NessieId, NessiePartitionDefinition> partitionDefinitions();

  Map<NessieId, NessieSortDefinition> sortOrders();

  Map<NessieId, DataAccessManifest> dataAccessManifests();

  Map<NessieId, BaseLocation> baseLocations();

  Map<NessieId, NessieTableSnapshot> tableSnapshots();

  Map<NessieId, NessieViewSnapshot> viewSnapshots();
}
