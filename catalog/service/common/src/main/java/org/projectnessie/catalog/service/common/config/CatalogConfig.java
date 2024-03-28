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
package org.projectnessie.catalog.service.common.config;

import java.util.Map;
import org.projectnessie.api.v2.params.ParsedReference;

public interface CatalogConfig {

  ParsedReference defaultBranch();

  /** Default warehouse configuration. This is used when a warehouse is not specified in a query. */
  WarehouseConfig defaultWarehouse();

  /**
   * Iceberg config defaults applicable to all clients and warehouses. Any properties that are
   * common to all iceberg clients should be included here. They will be passed to all clients on
   * all warehouses as config defaults. These defaults can be overridden on a per-warehouse basis,
   * see {@link WarehouseConfig#icebergConfigDefaults()}.
   */
  Map<String, String> icebergConfigDefaults();

  /**
   * Iceberg config overrides applicable to all clients and warehouses. Any properties that are
   * common to all iceberg clients should be included here. They will be passed to all clients on
   * all warehouses as config overrides. These overrides can be overridden on a per-warehouse basis,
   * see {@link WarehouseConfig#icebergConfigOverrides()}.
   */
  Map<String, String> icebergConfigOverrides();

  /** Map of warehouse names to warehouse configurations. */
  Map<String, ? extends WarehouseConfig> warehouses();

  /**
   * Attempts to match a warehouse by name or location. If no warehouse is found, the default
   * warehouse is returned.
   *
   * <p>Matching by location is required because the Iceberg REST API allows using either the name
   * or the location to identify a warehouse.
   */
  default WarehouseConfig getWarehouse(String warehouse) {
    WarehouseConfig w = warehouses().get(warehouse);
    if (w != null) {
      return w;
    }
    for (WarehouseConfig wc : warehouses().values()) {
      if (removeTrailingSlash(wc.location()).equals(removeTrailingSlash(warehouse))) {
        return wc;
      }
    }
    return defaultWarehouse();
  }

  private static String removeTrailingSlash(String s) {
    return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
  }
}
