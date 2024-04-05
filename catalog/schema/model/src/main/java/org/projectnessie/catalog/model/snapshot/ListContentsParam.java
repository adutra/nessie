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
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.immutables.value.Value;
import org.projectnessie.model.Namespace;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableListContentsParam.class)
@JsonDeserialize(as = ImmutableListContentsParam.class)
public interface ListContentsParam {

  ListContentsParam ROOT = listContentsParam(Namespace.EMPTY);

  static ListContentsParam.Builder builder() {
    return ImmutableListContentsParam.builder();
  }

  static ListContentsParam listContentsParam(Namespace namespace) {
    return listContentsParam(namespace, false, true, true, true);
  }

  static ListContentsParam listContentsParam(
      Namespace namespace,
      boolean recurse,
      boolean includeNamespaces,
      boolean includeTables,
      boolean includeViews) {
    return ImmutableListContentsParam.of(
        namespace, recurse, includeNamespaces, includeTables, includeViews);
  }

  @Value.Default
  default Namespace namespace() {
    return Namespace.EMPTY;
  }

  @Value.Default
  default boolean recurse() {
    return false;
  }

  @Value.Default
  default boolean includeNamespaces() {
    return true;
  }

  @Value.Default
  default boolean includeTables() {
    return true;
  }

  @Value.Default
  default boolean includeViews() {
    return true;
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder namespace(Namespace namespace);

    @CanIgnoreReturnValue
    Builder recurse(boolean recurse);

    @CanIgnoreReturnValue
    Builder includeNamespaces(boolean includeNamespaces);

    @CanIgnoreReturnValue
    Builder includeTables(boolean includeTables);

    @CanIgnoreReturnValue
    Builder includeViews(boolean includeViews);

    ListContentsParam build();
  }
}
