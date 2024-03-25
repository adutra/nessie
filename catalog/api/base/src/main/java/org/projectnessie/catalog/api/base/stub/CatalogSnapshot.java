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

import static org.projectnessie.catalog.api.base.transport.ContentRef.contentRef;

import java.util.List;
import org.projectnessie.catalog.model.NessieCatalog;
import org.projectnessie.catalog.model.ref.NessieCatalogSnapshot;
import org.projectnessie.catalog.model.ref.NessieMultipleSnapshots;
import org.projectnessie.catalog.model.ref.NessieTableSnapshotRef;
import org.projectnessie.catalog.model.ref.NessieTransaction;
import org.projectnessie.catalog.model.ref.NessieViewSnapshotRef;
import org.projectnessie.catalog.model.snapshot.FetchContentParam;
import org.projectnessie.catalog.model.snapshot.ListContentsParam;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;

class CatalogSnapshot implements NessieCatalogSnapshot {
  final CatalogStub stub;
  private final String refSpec;
  private Reference effectiveReference;

  CatalogSnapshot(CatalogStub stub, String refSpec) {
    this.stub = stub;
    this.refSpec = refSpec;
  }

  @Override
  public NessieCatalog catalog() {
    return stub;
  }

  @Override
  public String refSpec() {
    return refSpec;
  }

  @Override
  public NessieTableSnapshotRef tableSnapshot(ContentKey contentKey, FetchContentParam fetchParam) {
    return new TableSnapshotStub(stub, contentRef(null, refSpec, contentKey));
  }

  @Override
  public NessieViewSnapshotRef viewSnapshot(ContentKey contentKey, FetchContentParam fetchParam) {
    return new ViewSnapshotStub(stub, contentRef(null, refSpec, contentKey));
  }

  @Override
  public NessieMultipleSnapshots multipleByKey(
      List<ContentKey> contentKeys, FetchContentParam fetchParam) {
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }

  @Override
  public NessieMultipleSnapshots listContents(
      ListContentsParam param, FetchContentParam fetchParam) {
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }

  @Override
  public NessieTransaction startTransaction() {
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }
}
