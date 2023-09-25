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
package org.projectnessie.catalog.api.base.stub;

import org.projectnessie.catalog.api.base.transport.ContentRef;
import org.projectnessie.catalog.model.NessieView;
import org.projectnessie.catalog.model.ref.NessieViewSnapshotRef;
import org.projectnessie.catalog.model.snapshot.NessieViewSnapshot;

class ViewSnapshotStub extends EntitySnapshotStub<NessieViewSnapshot, NessieView>
    implements NessieViewSnapshotRef {

  ViewSnapshotStub(CatalogStub stub, ContentRef contentRef) {
    super(stub, contentRef);
  }

  @Override
  void fetch(ContentRef contentRef) {
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }
}
