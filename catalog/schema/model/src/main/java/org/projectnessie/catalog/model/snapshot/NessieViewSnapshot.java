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
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.projectnessie.catalog.model.NessieView;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.model.Content;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.nessie.immutables.NessieImmutable;

/**
 * Represents the state of a {@link NessieView} on a specific Nessie reference (commit).
 *
 * <p>The {@linkplain #snapshotId() ID of a table's snapshot} in the Nessie catalog is derived from
 * relevant fields in a concrete {@linkplain Content Nessie content object}, for example a {@link
 * IcebergTable} or {@link DeltaLakeTable}. This guarantees that each distinct state of a table is
 * represented by exactly one {@linkplain NessieViewSnapshot snapshot}. How exactly the {@linkplain
 * #snapshotId() ID} is derived is opaque to a user.
 */
@NessieImmutable
@JsonSerialize(as = ImmutableNessieViewSnapshot.class)
@JsonDeserialize(as = ImmutableNessieViewSnapshot.class)
public interface NessieViewSnapshot extends NessieEntitySnapshot<NessieView> {

  @Nullable
  @jakarta.annotation.Nullable
  NessieSchema currentSchema();

  /** The snapshots of tables and views referenced by this view. */
  List<NessieViewDependency> dependencies();

  @Override
  NessieView entity();

  static Builder builder() {
    return ImmutableNessieViewSnapshot.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(NessieViewSnapshot instance);

    @CanIgnoreReturnValue
    Builder snapshotId(NessieId snapshotId);

    @CanIgnoreReturnValue
    Builder putProperty(String key, String value);

    @CanIgnoreReturnValue
    Builder putProperty(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    Builder properties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllProperties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder entity(NessieView entity);

    @CanIgnoreReturnValue
    Builder currentSchema(@Nullable NessieSchema currentSchema);

    @CanIgnoreReturnValue
    Builder addDependency(NessieViewDependency element);

    @CanIgnoreReturnValue
    Builder addDependencies(NessieViewDependency... elements);

    @CanIgnoreReturnValue
    Builder dependencies(Iterable<? extends NessieViewDependency> elements);

    @CanIgnoreReturnValue
    Builder addAllDependencies(Iterable<? extends NessieViewDependency> elements);

    NessieViewSnapshot build();
  }
}
