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
package org.projectnessie.catalog.api.rest.spec;

import java.util.List;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;

/**
 * Base interface for Nessie Catalog REST service providing the Java signatures.
 *
 * <p>Must not add Jakarta-WS-RS annotations, because that would cause ambiguous resource
 * declarations, because generic types are erased to {@link Object} and then clash with the async
 * {@code Uni<>} in the server.
 */
public interface NessieCatalogServiceBase<SINGLE_RESPONSE, MULTI_RESPONSE> {
  MULTI_RESPONSE tableSnapshots(
      String ref, List<ContentKey> keys, String format, String specVersion)
      throws NessieNotFoundException;

  SINGLE_RESPONSE tableSnapshot(String ref, ContentKey key, String format, String specVersion)
      throws NessieNotFoundException;

  SINGLE_RESPONSE manifestList(String ref, ContentKey key, String format, String specVersion)
      throws NessieNotFoundException;

  SINGLE_RESPONSE manifestFile(
      String ref, ContentKey key, String format, String specVersion, String manifestFile)
      throws NessieNotFoundException;

  SINGLE_RESPONSE dataFile(
      String ref, ContentKey key, String fileType, String fileToken, String dataFile)
      throws NessieNotFoundException;
}
