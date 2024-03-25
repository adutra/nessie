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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URL;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import org.projectnessie.catalog.model.NessieCatalog;

public interface NessieCatalogAPI {

  static Builder builder() {
    return NessieCatalogAPIImpl.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {

    @CanIgnoreReturnValue
    Builder withPropertiesSource(String location);

    @CanIgnoreReturnValue
    Builder withClassPathSource(String location);

    @CanIgnoreReturnValue
    Builder withProperties(Properties properties, String sourceName);

    @CanIgnoreReturnValue
    Builder withProperties(Map<String, String> propertiesMap, String sourceName);

    @CanIgnoreReturnValue
    Builder withProperties(Map<String, String> propertiesMap, String sourceName, int ordinal);

    @CanIgnoreReturnValue
    Builder withYamlSource(Path source);

    @CanIgnoreReturnValue
    Builder withYamlSource(Path source, int ordinal);

    @CanIgnoreReturnValue
    Builder withYamlSource(URL source);

    @CanIgnoreReturnValue
    Builder withYamlSource(URL source, int ordinal);

    NessieCatalogAPI build();
  }

  NessieCatalog connect();
}
