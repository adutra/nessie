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
import java.util.List;
import javax.annotation.Nullable;
import org.projectnessie.nessie.immutables.NessieImmutable;

public interface DataPredicate {

  @NessieImmutable
  @JsonSerialize(as = ImmutablePartitionIdentifier.class)
  @JsonDeserialize(as = ImmutablePartitionIdentifier.class)
  interface SinglePartition {
    PartitionIdentifier partitionIdentifier();
  }

  @NessieImmutable
  @JsonSerialize(as = ImmutableMultiplePartitions.class)
  @JsonDeserialize(as = ImmutableMultiplePartitions.class)
  interface MultiplePartitions {
    List<PartitionIdentifier> partitionIdentifier();
  }

  @NessieImmutable
  @JsonSerialize(as = ImmutablePartitionRange.class)
  @JsonDeserialize(as = ImmutablePartitionRange.class)
  interface PartitionRange {
    @Nullable
    @jakarta.annotation.Nullable
    PartitionIdentifier fromPartition();

    boolean fromIncluding();

    @Nullable
    @jakarta.annotation.Nullable
    PartitionIdentifier toPartition();

    boolean toIncluding();
  }
}
