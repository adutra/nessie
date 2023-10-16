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
package org.projectnessie.catalog.formats.iceberg.manifest;

import org.projectnessie.catalog.model.manifest.NessieFileStatus;

public enum IcebergManifestEntryStatus {
  EXISTING(NessieFileStatus.EXISTING),
  ADDED(NessieFileStatus.ADDED),
  DELETED(NessieFileStatus.DELETED);

  private final NessieFileStatus nessieFileStatus;

  IcebergManifestEntryStatus(NessieFileStatus nessieFileStatus) {
    this.nessieFileStatus = nessieFileStatus;
  }

  public int intValue() {
    return ordinal();
  }

  public NessieFileStatus nessieFileStatus() {
    return nessieFileStatus;
  }

  public static IcebergManifestEntryStatus fromNessieFileStatus(NessieFileStatus nessieFileStatus) {
    switch (nessieFileStatus) {
      case ADDED:
        return ADDED;
      case EXISTING:
        return EXISTING;
      case DELETED:
        return DELETED;
      default:
        throw new IllegalArgumentException(
            "Unknown data file status for Iceberg: " + nessieFileStatus);
    }
  }
}
