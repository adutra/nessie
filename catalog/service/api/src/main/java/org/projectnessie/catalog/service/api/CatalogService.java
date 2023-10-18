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
import java.util.Optional;
import java.util.OptionalInt;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;

public interface CatalogService {

  /**
   * Retrieves table-snapshot related information, including data file manifests.
   *
   * @param ref Nessie reference specification.
   * @param key The table's content key
   * @param manifestFileId ID of the manifest file, if retrieving a manifest file.
   * @param format Requested kind and representation of the table snapshot related information.
   * @param specVersion Version of the specification to use in the returned data.
   * @param catalogUriResolver produces URIs for related entities, like Iceberg manifest lists or
   *     manifest files.
   * @return The response is either a response object or callback to produce the result. The latter
   *     is useful to return results that are quite big, for example Iceberg manifest lists or
   *     manifest files.
   */
  SnapshotResponse retrieveTableSnapshot(
      String ref,
      ContentKey key,
      Optional<NessieId> manifestFileId,
      SnapshotFormat format,
      OptionalInt specVersion,
      CatalogUriResolver catalogUriResolver)
      throws NessieNotFoundException;

  interface CatalogUriResolver {
    URI icebergManifestList();

    URI icebergManifestFile(NessieId manifestFileId);
  }
}
