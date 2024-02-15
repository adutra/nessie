/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.service.rest;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.URI;
import java.net.URLEncoder;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.manifest.NessieDataFileFormat;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SnapshotFormat;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;

class CatalogUriResolverImpl implements CatalogService.CatalogUriResolver {
  private final URI baseUri;
  private final SnapshotFormat snapshotFormat;

  CatalogUriResolverImpl(URI requestUri, SnapshotFormat snapshotFormat) {
    this.snapshotFormat = snapshotFormat;
    baseUri = requestUri.resolve("../..");
  }

  @Override
  public URI icebergManifestList(
      Reference effectiveReference, ContentKey key, NessieTableSnapshot snapshot) {
    // TODO memoize toPathString representations
    return snapshotFormat.asImported()
        ? URI.create(snapshot.icebergManifestListLocation())
        : baseUri.resolve(
            effectiveReference.toPathString()
                + "/manifest-list/"
                + key.toPathString()
                + "?x=.avro");
  }

  @Override
  public URI icebergManifestFile(
      Reference effectiveReference, ContentKey key, NessieId manifestFileId) {
    // TODO memoize toPathString representations
    return baseUri.resolve(
        effectiveReference.toPathString()
            + "/manifest-file/"
            + key.toPathString()
            + "?manifest-file="
            + URLEncoder.encode(manifestFileId.idAsBase64(), UTF_8)
            + ".avro");
  }

  @Override
  public URI dataFile(
      Reference effectiveReference,
      ContentKey key,
      NessieDataFileFormat fileFormat,
      String dataFile) {
    // TODO generate dataFile token
    String fileToken = "tokenToVerifyThatTheDataFileBelongsToTheTable";
    String fileFormatParam =
        dataFile.endsWith(fileFormat.fileExtension())
            ? ""
            : "&format=" + URLEncoder.encode(fileFormat.name(), UTF_8);
    // TODO memoize toPathString representations
    return baseUri.resolve(
        effectiveReference.toPathString()
            + "/data-file/"
            + key.toPathString()
            + "?token="
            + URLEncoder.encode(fileToken, UTF_8)
            // Keep 'file' parameter at the end
            + "&file="
            + URLEncoder.encode(dataFile, UTF_8)
            + fileFormatParam);
  }
}
