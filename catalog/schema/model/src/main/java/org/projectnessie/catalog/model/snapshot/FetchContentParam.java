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

import static java.util.Collections.emptySet;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Set;
import org.immutables.value.Value;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableFetchContentParam.class)
@JsonDeserialize(as = ImmutableFetchContentParam.class)
public interface FetchContentParam {
  /**
   * The (set of) properties that shall be prefetched. An empty set means that no properties will be
   * eagerly fetched, roughly equivalent to an existence check.
   */
  @Value.Default
  default Set<Prefetch> prefetches() {
    return emptySet();
  }

  static FetchContentParam fetchContentParam(Set<Prefetch> prefetches) {
    return ImmutableFetchContentParam.of(prefetches);
  }
}
