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

import java.util.Set;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public interface RequestObjects {
  Set<NessieId> tables();

  Set<NessieId> views();

  Set<NessieId> schemas();

  Set<NessieId> partitionDefinitions();

  Set<NessieId> sortDefinitions();

  Set<NessieId> dataAccessManifests();

  Set<NessieId> baseLocations();

  Set<NessieId> tableSnapshots();

  Set<NessieId> viewSnapshots();
}
