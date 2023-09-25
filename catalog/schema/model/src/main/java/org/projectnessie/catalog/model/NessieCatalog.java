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
package org.projectnessie.catalog.model;

import java.net.URI;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.locations.BaseLocation;
import org.projectnessie.catalog.model.ref.NessieCatalogSnapshot;

/**
 * Represents a Nessie catalog.
 *
 * <p>Changes to the catalog's content must refer to a {@linkplain #forRefSpec(String) Nessie
 * reference}.
 *
 * <p>Beside the Nessie reference related content, there are a couple of global settings, like base
 * locations.
 */
public interface NessieCatalog extends AutoCloseable {
  NessieCatalogSnapshot forRefSpec(String refSpec);

  BaseLocation defaultBaseLocation();

  BaseLocation baseLocationById(NessieId id);

  BaseLocation createBaseLocation(String name, URI baseUri);

  BaseLocation deactivateBaseLocation(NessieId id);

  BaseLocation activateBaseLocation(NessieId id);
}
