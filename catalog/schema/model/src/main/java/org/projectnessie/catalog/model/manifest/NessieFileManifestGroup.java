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
package org.projectnessie.catalog.model.manifest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.List;
import org.projectnessie.nessie.immutables.NessieImmutable;

// Corresponds to a manifest-list
@NessieImmutable
@JsonSerialize(as = ImmutableNessieFileManifestGroup.class)
@JsonDeserialize(as = ImmutableNessieFileManifestGroup.class)
public interface NessieFileManifestGroup {
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  // TODO Store manifest-list externally, in a way that we can update the manifest-list w/o touching
  //  the persisted snapshot.
  // TODO Can we derive the ID of the manifest list from the ID of the snapshot?
  // TODO Find a way to put multiple NessieListManifestEntry in a database row.
  List<NessieFileManifestGroupEntry> manifests();

  static Builder builder() {
    return ImmutableNessieFileManifestGroup.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(NessieFileManifestGroup fileManifestGroup);

    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder addManifest(NessieFileManifestGroupEntry element);

    @CanIgnoreReturnValue
    Builder addManifests(NessieFileManifestGroupEntry... elements);

    @CanIgnoreReturnValue
    Builder manifests(Iterable<? extends NessieFileManifestGroupEntry> elements);

    @CanIgnoreReturnValue
    Builder addAllManifests(Iterable<? extends NessieFileManifestGroupEntry> elements);

    NessieFileManifestGroup build();
  }
}
