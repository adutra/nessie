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
package org.projectnessie.catalog.service.api;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.manifest.NessieDataFileFormat;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;

public interface CatalogService {

  /**
   * Retrieves table-snapshot related information, including data file manifests.
   *
   * @param reqParams Parameters holding the Nessie reference specification, snapshot format and
   *     more.
   * @param key The table's content key
   * @param catalogUriResolver produces URIs for related entities, like Iceberg manifest lists or
   *     manifest files.
   * @return The response is either a response object or callback to produce the result. The latter
   *     is useful to return results that are quite big, for example Iceberg manifest lists or
   *     manifest files.
   */
  CompletionStage<SnapshotResponse> retrieveTableSnapshot(
      SnapshotReqParams reqParams, ContentKey key, CatalogUriResolver catalogUriResolver)
      throws NessieNotFoundException;

  Stream<Supplier<CompletionStage<SnapshotResponse>>> retrieveTableSnapshots(
      SnapshotReqParams reqParams,
      List<ContentKey> keys,
      CatalogUriResolver catalogUriResolver,
      Consumer<Reference> effectiveReferenceConsumer)
      throws NessieNotFoundException;

  interface CatalogUriResolver {
    URI icebergManifestList(
        Reference effectiveReference, ContentKey key, NessieTableSnapshot snapshot);

    URI icebergManifestFile(Reference effectiveReference, ContentKey key, NessieId manifestFileId);

    URI dataFile(
        Reference effectiveReference,
        ContentKey key,
        NessieDataFileFormat fileFormat,
        String dataFile);
  }
}
