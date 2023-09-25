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
package org.projectnessie.catalog.api.direct;

import java.net.URI;
import java.util.List;
import java.util.Set;
import org.projectnessie.catalog.api.base.transport.CatalogTransport;
import org.projectnessie.catalog.api.base.transport.ContentRef;
import org.projectnessie.catalog.api.base.transport.KnownDescriptors;
import org.projectnessie.catalog.api.base.transport.RequestObjects;
import org.projectnessie.catalog.api.base.transport.ResultObjects;
import org.projectnessie.catalog.api.base.transport.TableSnapshot;
import org.projectnessie.catalog.api.base.transport.ViewSnapshot;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.locations.BaseLocation;
import org.projectnessie.catalog.model.snapshot.DataManifest;
import org.projectnessie.catalog.model.snapshot.DataPredicate;
import org.projectnessie.catalog.model.snapshot.Prefetch;

class DirectCatalogTransport implements CatalogTransport {

  DirectCatalogTransport() {}

  @Override
  public void close() {}

  @Override
  public String transportName() {
    return DirectCatalogTransportFactory.NAME;
  }

  @Override
  public BaseLocation defaultBaseLocation() {
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }

  @Override
  public BaseLocation baseLocationById(NessieId id) {
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }

  @Override
  public BaseLocation createBaseLocation(String name, URI baseUri) {
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }

  @Override
  public BaseLocation deactivateBaseLocation(NessieId id) {
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }

  @Override
  public BaseLocation activateBaseLocation(NessieId id) {
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }

  @Override
  public KnownDescriptors.Builder knownDescriptorsBuilder() {
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }

  @Override
  public TableSnapshot tableSnapshot(
      KnownDescriptors knownDescriptors, ContentRef contentRef, Set<Prefetch> prefetch) {
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }

  @Override
  public ViewSnapshot viewSnapshot(KnownDescriptors knownDescriptors, ContentRef contentRef) {
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }

  @Override
  public DataManifest dataManifest(
      KnownDescriptors knownDescriptors, ContentRef contentRef, List<DataPredicate> predicates) {
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }

  @Override
  public ResultObjects requestDescribingObjects(RequestObjects requestObjects) {
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }
}
