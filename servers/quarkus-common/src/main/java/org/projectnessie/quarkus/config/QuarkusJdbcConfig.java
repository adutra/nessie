/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.quarkus.config;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import java.util.Optional;
import org.projectnessie.versioned.storage.jdbc.JdbcBackendBaseConfig;

/**
 * Setting {@code nessie.version.store.type=JDBC} enables transactional/RDBMS as the version store
 * used by the Nessie server. Configuration of the datastore will be done by Quarkus and depends on
 * many factors, such as the actual database in use. A complete set of JDBC configuration options
 * can be found on <a href="https://quarkus.io/guides/datasource">quarkus.io</a>.
 */
@StaticInitSafe
@ConfigMapping(prefix = "nessie.version.store.persist.jdbc")
public interface QuarkusJdbcConfig extends JdbcBackendBaseConfig {

  @Override
  Optional<String> catalog();

  @Override
  Optional<String> schema();
}
