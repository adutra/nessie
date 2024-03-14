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
package org.projectnessie.catalog.api.base;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.net.URI;
import java.util.Optional;
import org.projectnessie.catalog.api.base.transport.CatalogTransportFactory;

// TODO this interface appears as a configurable in Quarkus Server (autocompletion in application.properties).
//  Need to refactor this nessie-catalog-api-base module.
@ConfigMapping(prefix = "nessie.catalog")
public interface NessieCatalogClientConfig {
  Transport transport();

  URI uri();

  Ref ref();

  @ConfigMapping(prefix = "nessie.catalog.transport")
  interface Transport {
    @WithDefault("REST")
    String name();

    Optional<Class<? extends CatalogTransportFactory<?>>> type();

    String toString();
  }

  interface Ref {
    @WithDefault("main")
    String name();

    Optional<String> hash();

    String toString();
  }

  String toString();
}
