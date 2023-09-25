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
package org.projectnessie.catalog.api.base.stub;

import java.net.URI;
import org.projectnessie.catalog.api.base.transport.CatalogTransport;
import org.projectnessie.catalog.api.base.transport.KnownDescriptors;
import org.projectnessie.catalog.model.NessieCatalog;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.locations.BaseLocation;
import org.projectnessie.catalog.model.ref.NessieCatalogSnapshot;

public class CatalogStub implements NessieCatalog {
  final CatalogTransport transport;

  public CatalogStub(CatalogTransport transport) {
    this.transport = transport;
  }

  @Override
  public void close() throws Exception {
    transport.close();
  }

  @Override
  public BaseLocation defaultBaseLocation() {
    return transport.defaultBaseLocation();
  }

  @Override
  public BaseLocation baseLocationById(NessieId id) {
    return transport.baseLocationById(id);
  }

  @Override
  public BaseLocation createBaseLocation(String name, URI baseUri) {
    return transport.createBaseLocation(name, baseUri);
  }

  @Override
  public BaseLocation deactivateBaseLocation(NessieId id) {
    return transport.deactivateBaseLocation(id);
  }

  @Override
  public BaseLocation activateBaseLocation(NessieId id) {
    return transport.activateBaseLocation(id);
  }

  @Override
  public NessieCatalogSnapshot forRefSpec(String refSpec) {
    return new CatalogSnapshot(this, refSpec);
  }

  public KnownDescriptors knownDescriptors() {
    return transport
        .knownDescriptorsBuilder()
        // TODO
        .build();
  }
}
