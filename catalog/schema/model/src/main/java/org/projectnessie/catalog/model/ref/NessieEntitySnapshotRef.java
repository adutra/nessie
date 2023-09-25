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
package org.projectnessie.catalog.model.ref;

import org.projectnessie.catalog.model.NessieEntity;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;

public interface NessieEntitySnapshotRef<
    ES extends NessieEntitySnapshot<E>, E extends NessieEntity> {

  Reference reference();

  /**
   * The content key of the {@linkplain #snapshot() snapshot} on the {@linkplain #reference() Nessie
   * reference}.
   */
  ContentKey contentKey();

  ES snapshot();

  CommitMeta commitMeta();
}
