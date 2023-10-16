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

import org.projectnessie.catalog.model.manifest.NessieFileContentType;

public enum IcebergDataContent {
  DATA(NessieFileContentType.ICEBERG_DATA_FILE),
  POSITION_DELETES(NessieFileContentType.ICEBERG_POSITION_DELETES_FILE),
  EQUALITY_DELETES(NessieFileContentType.ICEBERG_EQUALITY_DELETES_FILE);

  private final NessieFileContentType nessieFileContentType;

  IcebergDataContent(NessieFileContentType nessieFileContentType) {
    this.nessieFileContentType = nessieFileContentType;
  }

  public NessieFileContentType nessieFileContentType() {
    return nessieFileContentType;
  }

  public static IcebergDataContent fromNessieFileContentType(
      NessieFileContentType nessieFileContentType) {
    switch (nessieFileContentType) {
      case ICEBERG_DATA_FILE:
        return DATA;
      case ICEBERG_EQUALITY_DELETES_FILE:
        return EQUALITY_DELETES;
      case ICEBERG_POSITION_DELETES_FILE:
        return POSITION_DELETES;
      default:
        throw new IllegalArgumentException(
            "Unsupported file content type for Iceberg: " + nessieFileContentType);
    }
  }
}
