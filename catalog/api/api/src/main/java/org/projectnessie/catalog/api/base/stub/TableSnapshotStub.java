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

import java.util.Collections;
import org.projectnessie.catalog.api.base.transport.ContentRef;
import org.projectnessie.catalog.api.base.transport.TableSnapshot;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.ref.NessieTableSnapshotRef;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;

class TableSnapshotStub extends EntitySnapshotStub<NessieTableSnapshot, NessieTable>
    implements NessieTableSnapshotRef {

  TableSnapshotStub(CatalogStub stub, ContentRef contentRef) {
    super(stub, contentRef);
  }

  @Override
  void fetch(ContentRef contentRef) {
    TableSnapshot snapshot =
        stub.transport.tableSnapshot(stub.knownDescriptors(), contentRef, Collections.emptySet());

    NessieTableSnapshot.Builder nessieTableSnapshot =
        NessieTableSnapshot.builder()
            .from(snapshot.shallowSnapshot())
            .schemas(snapshot.schemas())
            .partitionDefinitions(snapshot.partitionDefinitions())
            .sortDefinitions(snapshot.sortDefinitions());

    // TODO go through the list of `snapshot.omitted*()` and populate those from the cache

    fetched(snapshot.commitMeta(), contentRef, nessieTableSnapshot.build());
  }
}
