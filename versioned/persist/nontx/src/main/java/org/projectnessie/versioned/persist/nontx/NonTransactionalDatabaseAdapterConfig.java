/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.versioned.persist.nontx;

import org.immutables.value.Value;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.DatabaseConnectionProvider;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStateLogEntry;

public interface NonTransactionalDatabaseAdapterConfig<T extends DatabaseConnectionProvider<?>>
    extends DatabaseAdapterConfig<T> {
  int DEFAULT_PARENTS_PER_GLOBAL_COMMIT = 50;

  /**
   * The number of parent-global-commit-hashes stored in {@link
   * GlobalStateLogEntry#getParentsList()}. Defaults to {@value #DEFAULT_PARENTS_PER_GLOBAL_COMMIT}.
   */
  @Value.Default
  default int getParentsPerGlobalCommit() {
    return DEFAULT_PARENTS_PER_GLOBAL_COMMIT;
  }

  NonTransactionalDatabaseAdapterConfig<T> withParentsPerGlobalCommit(int parentsPerGlobalCommit);
}
