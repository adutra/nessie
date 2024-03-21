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

import static org.projectnessie.api.v2.params.ReferenceResolver.resolveReferencePathElement;
import static org.projectnessie.model.Validation.REF_NAME_PATH_ELEMENT_REGEX;

import java.net.URI;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;

public interface ExternalBaseUri {
  URI externalBaseURI();

  default URI coreRootURI() {
    return externalBaseURI().resolve("api/");
  }

  default URI catalogBaseURI() {
    return externalBaseURI().resolve("catalog/v1/");
  }

  default URI icebergBaseURI() {
    return externalBaseURI().resolve("iceberg/");
  }

  default boolean isNessieCatalogUri(String uri) {
    return uri.startsWith(externalBaseURI().toString());
  }

  Pattern SNAPSHOT_URI_PATTERN =
      Pattern.compile(".*/trees/" + REF_NAME_PATH_ELEMENT_REGEX + "/snapshot/(.*)$");

  default Optional<TableRef> resolveTableFromUri(String uri) {
    URI u = URI.create(uri);
    Matcher m = SNAPSHOT_URI_PATTERN.matcher(u.getPath());
    if (!m.matches()) {
      return Optional.empty();
    }

    ContentKey key = ContentKey.fromPathString(m.group(3));
    ParsedReference ref = parseRefPathString(m.group(1));

    return Optional.of(TableRef.tableRef(key, ref, null));
  }

  static ParsedReference parseRefPathString(String refPathString) {
    return resolveReferencePathElement(
        refPathString,
        Reference.ReferenceType.BRANCH,
        () -> {
          throw new IllegalArgumentException("ref path must specify a branch");
        });
  }
}
