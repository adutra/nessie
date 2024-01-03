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

/**
 * The output format of snapshot related entity information that is returned to actual clients, with
 * attributes whether additional entity metadata like schema, partition definition, sort definitions
 * and/or file-group entries are returned.
 */
public enum SnapshotFormat {
  /**
   * The Nessie Catalog main native format includes the entity snapshot information with schemas,
   * partition definitions, sort definitions and file-group entries.
   */
  NESSIE_SNAPSHOT(false, true, true),
  /** Iceberg table metadata. */
  ICEBERG_TABLE_METADATA(false, true, false),
  /** Iceberg table metadata, returning the original paths from the imported metadata file. */
  ICEBERG_TABLE_METADATA_IMPORTED(true, true, false),
  /** Iceberg manifest list. */
  ICEBERG_MANIFEST_LIST(false, false, true),
  /** Iceberg manifest list, returning the original paths from the imported metadata file. */
  ICEBERG_MANIFEST_LIST_IMPORTED(true, false, true),
  // TODO Consider not exposing a manifest file directly, but only import it. OTOH it might be
  //  useful to rewrite the paths to the data files - in other words to let the Nessie Catalog do a
  //  base-path/location rewrite "on the fly".
  ICEBERG_MANIFEST_FILE(true, false, true),
  ;

  private final boolean useOriginalPaths;
  private final boolean includesEntityMetadata;
  private final boolean includesFileManifestGroup;

  SnapshotFormat(
      boolean useOriginalPaths, boolean includesEntityMetadata, boolean includesFileManifestGroup) {
    this.useOriginalPaths = useOriginalPaths;
    this.includesEntityMetadata = includesEntityMetadata;
    this.includesFileManifestGroup = includesFileManifestGroup;
  }

  public boolean useOriginalPaths() {
    return useOriginalPaths;
  }

  public boolean includesFileManifestGroup() {
    return includesFileManifestGroup;
  }

  public boolean includesEntityMetadata() {
    return includesEntityMetadata;
  }
}
