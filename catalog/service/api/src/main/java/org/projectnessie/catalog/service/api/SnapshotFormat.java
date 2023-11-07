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

public enum SnapshotFormat {
  NESSIE_SNAPSHOT(false),
  ICEBERG_TABLE_METADATA(false),
  ICEBERG_TABLE_METADATA_IMPORTED(true),
  ICEBERG_MANIFEST_LIST(false),
  ICEBERG_MANIFEST_LIST_IMPORTED(true),
  ICEBERG_MANIFEST_FILE(true),
  ;

  private final boolean useOriginalPaths;

  SnapshotFormat(boolean useOriginalPaths) {
    this.useOriginalPaths = useOriginalPaths;
  }

  public boolean useOriginalPaths() {
    return useOriginalPaths;
  }
}
