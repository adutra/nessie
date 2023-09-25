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

import java.net.URI;
import java.util.List;
import java.util.Set;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.locations.BaseLocation;
import org.projectnessie.catalog.model.snapshot.DataManifest;
import org.projectnessie.catalog.model.snapshot.DataPredicate;
import org.projectnessie.catalog.model.snapshot.Prefetch;

public interface CatalogTransport extends AutoCloseable {

  String transportName();

  BaseLocation defaultBaseLocation();

  BaseLocation baseLocationById(NessieId id);

  BaseLocation createBaseLocation(String name, URI baseUri);

  BaseLocation deactivateBaseLocation(NessieId id);

  BaseLocation activateBaseLocation(NessieId id);

  KnownDescriptors.Builder knownDescriptorsBuilder();

  TableSnapshot tableSnapshot(
      KnownDescriptors knownDescriptors, ContentRef contentRef, Set<Prefetch> prefetch);

  ViewSnapshot viewSnapshot(KnownDescriptors knownDescriptors, ContentRef contentRef);

  DataManifest dataManifest(
      KnownDescriptors knownDescriptors, ContentRef contentRef, List<DataPredicate> predicates);

  ResultObjects requestDescribingObjects(RequestObjects requestObjects);
}
