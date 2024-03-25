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
package org.projectnessie.catalog.api.base.transport;

import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.List;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.NessieView;
import org.projectnessie.catalog.model.snapshot.NessieViewDependency;
import org.projectnessie.catalog.model.snapshot.NessieViewSnapshot;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonTypeName("VIEW")
// Suppress: "Constructor parameters should be better defined on the same level of inheritance
// hierarchy..."
@SuppressWarnings("immutables:subtype")
public interface ViewSnapshot extends EntitySnapshot<NessieViewSnapshot, NessieView> {

  @Override
  @Value.NonAttribute
  default String type() {
    return "VIEW";
  }

  @Override
  NessieViewSnapshot shallowSnapshot();

  List<NessieViewDependency> dependencies();
}
