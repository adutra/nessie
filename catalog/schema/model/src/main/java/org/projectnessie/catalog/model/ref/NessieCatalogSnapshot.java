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

import java.util.List;
import org.projectnessie.catalog.model.NessieCatalog;
import org.projectnessie.catalog.model.snapshot.FetchContentParam;
import org.projectnessie.catalog.model.snapshot.ListContentsParam;
import org.projectnessie.model.ContentKey;

/**
 * Represents the state of the {@link #catalog() Nessie catalog} on a specific Nessie {@linkplain
 * #refSpec() reference}.
 */
public interface NessieCatalogSnapshot {
  NessieCatalog catalog();

  /** The reference in Nessie against which this {@link NessieCatalogSnapshot} . */
  String refSpec();

  /** Retrieve a table snapshot by its content key. */
  NessieTableSnapshotRef tableSnapshot(ContentKey contentKey, FetchContentParam fetchParam);

  /** Retrieve a view snapshot by its content key. */
  NessieViewSnapshotRef viewSnapshot(ContentKey contentKey, FetchContentParam fetchParam);

  /** Retrieve a multiple snapshots by their content keys. */
  NessieMultipleSnapshots multipleByKey(List<ContentKey> contentKeys, FetchContentParam fetchParam);

  /** List contents of a namespace. */
  NessieMultipleSnapshots listContents(ListContentsParam param, FetchContentParam fetchParam);

  NessieTransaction startTransaction();
}
