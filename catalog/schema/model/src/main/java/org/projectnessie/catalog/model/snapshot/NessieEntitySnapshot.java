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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeId;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Map;
import org.projectnessie.catalog.model.NessieEntity;
import org.projectnessie.catalog.model.id.NessieId;

/**
 * Common entity, table or view, attributes for a given snapshot of that entity. An entity snapshot
 * is the state of an entity on a particular Nessie commit.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = NessieTableSnapshot.class, name = "TABLE"),
  @JsonSubTypes.Type(value = NessieViewSnapshot.class, name = "VIEW")
})
public interface NessieEntitySnapshot<E extends NessieEntity> {
  @JsonTypeId
  String type();

  NessieId snapshotId();

  Map<String, String> properties();

  E entity();
}
