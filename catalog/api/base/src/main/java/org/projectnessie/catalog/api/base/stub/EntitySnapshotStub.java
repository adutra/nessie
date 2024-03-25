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

import org.projectnessie.catalog.api.base.transport.ContentRef;
import org.projectnessie.catalog.model.NessieEntity;
import org.projectnessie.catalog.model.ref.NessieEntitySnapshotRef;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;

abstract class EntitySnapshotStub<ES extends NessieEntitySnapshot<E>, E extends NessieEntity>
    implements NessieEntitySnapshotRef<ES, E> {
  final CatalogStub stub;
  private ContentRef contentRef;

  private ES entitySnapshot;
  private CommitMeta commitMeta;

  EntitySnapshotStub(CatalogStub stub, ContentRef contentRef) {
    this.stub = stub;
    this.contentRef = contentRef;
  }

  void fetched(CommitMeta commitMeta, ContentRef contentRef, ES entitySnapshot) {
    this.commitMeta = commitMeta;
    this.contentRef = contentRef;
    this.entitySnapshot = entitySnapshot;
  }

  abstract void fetch(ContentRef contentRef);

  @Override
  public CommitMeta commitMeta() {
    if (commitMeta == null) {
      fetch(contentRef);
    }
    return commitMeta;
  }

  @Override
  public Reference reference() {
    if (contentRef.reference() == null) {
      fetch(contentRef);
    }
    return contentRef.reference();
  }

  @Override
  public ContentKey contentKey() {
    return contentRef.contentKey();
  }

  @Override
  public ES snapshot() {
    if (entitySnapshot == null) {
      fetch(contentRef);
    }
    return entitySnapshot;
  }
}
