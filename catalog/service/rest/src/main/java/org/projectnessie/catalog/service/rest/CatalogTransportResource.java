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
package org.projectnessie.catalog.service.rest;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import org.projectnessie.catalog.api.rest.spec.NessieCatalogService;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;

@RequestScoped
@jakarta.enterprise.context.RequestScoped
public class CatalogTransportResource implements NessieCatalogService {

  private final CatalogService catalogService;

  public CatalogTransportResource() {
    this(null);
  }

  @Inject
  @jakarta.inject.Inject
  public CatalogTransportResource(CatalogService catalogService) {
    this.catalogService = catalogService;
  }

  @Override
  public Object tableSnapshot(String ref, ContentKey key, String format, String specVersion)
      throws NessieNotFoundException {
    return catalogService.tableSnapshot(ref, key, format, specVersion);
  }
}
