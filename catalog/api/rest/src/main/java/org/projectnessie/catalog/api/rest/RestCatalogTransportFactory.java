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
package org.projectnessie.catalog.api.rest;

import java.util.Optional;
import org.projectnessie.catalog.api.base.NessieCatalogClientConfig;
import org.projectnessie.catalog.api.base.transport.CatalogTransport;
import org.projectnessie.catalog.api.base.transport.CatalogTransportFactory;
import org.projectnessie.catalog.api.rest.spec.NessieCatalogService;

public class RestCatalogTransportFactory
    implements CatalogTransportFactory<RestCatalogTransportConfig> {

  static final String NAME = "REST";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public CatalogTransport createTransport(
      NessieCatalogClientConfig stringStringMap,
      Optional<RestCatalogTransportConfig> transportConfig) {
    NessieCatalogService service = null;
    return new RestCatalogTransport(service);
  }

  @Override
  public Optional<Class<RestCatalogTransportConfig>> configMappingType() {
    return Optional.of(RestCatalogTransportConfig.class);
  }
}
