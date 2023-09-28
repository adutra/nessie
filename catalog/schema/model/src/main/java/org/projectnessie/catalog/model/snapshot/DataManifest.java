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
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.catalog.model.schema.NessieSortDefinition;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableDataManifest.class)
@JsonDeserialize(as = ImmutableDataManifest.class)
public interface DataManifest {
  List<NessieSchema> involvedSchemas();

  List<NessiePartitionDefinition> involvedPartitionDefinitions();

  List<NessieSortDefinition> involvedSortDefinitions();

  List<DataAccessManifest> dataAccessManifests();
}
