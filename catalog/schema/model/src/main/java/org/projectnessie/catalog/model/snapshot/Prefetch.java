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
package org.projectnessie.catalog.model.snapshot;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.projectnessie.nessie.immutables.NessieImmutable;

/**
 * Base interface to specify the kind of data that shall be prefetched to let the catalog
 * implementation use optimized access/fetch paths to improve the execution time.
 */
@NessieImmutable
@JsonSerialize(as = ImmutablePrefetch.class)
@JsonDeserialize(as = ImmutablePrefetch.class)
public interface Prefetch {

  String name();

  /** Used to instruct the catalog to prefetch the current schema. */
  Prefetch CURRENT_SCHEMA = prefetch("CURRENT_SCHEMA");

  /**
   * Used to instruct the catalog to prefetch the current schema and other current settings, for
   * example the Iceberg partition-spec and sort-order.
   */
  Prefetch CURRENT_ALL = prefetch("CURRENT_ALL");

  static Prefetch prefetch(String name) {
    return ImmutablePrefetch.of(name);
  }
}
