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
package org.projectnessie.catalog.service.server.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithConverter;
import io.smallrye.config.WithName;
import java.util.Map;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.service.common.config.CatalogConfig;

@ConfigMapping(prefix = "nessie.catalog")
public interface QuarkusCatalogConfig extends CatalogConfig {
  @Override
  @WithName("default-branch")
  @WithConverter(ParsedReferenceConverter.class)
  ParsedReference defaultBranch();

  @Override
  @WithName("default-warehouse")
  QuarkusWarehouseConfig defaultWarehouse();

  @Override
  @WithName("warehouses")
  Map<String, QuarkusWarehouseConfig> warehouses();

  @Override
  @WithName("iceberg-client-core-properties")
  Map<String, String> icebergClientCoreProperties();
}
