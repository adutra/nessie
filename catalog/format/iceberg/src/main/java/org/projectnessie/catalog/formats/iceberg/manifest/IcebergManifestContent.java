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

public enum IcebergManifestContent {
  DATA("data", 0, NessieFileContentType.ICEBERG_DATA_FILE),
  DELETES("deletes", 1, NessieFileContentType.ICEBERG_DELETE_FILE);

  private final String stringValue;
  private final int intValue;
  private final NessieFileContentType nessieFileContentType;

  IcebergManifestContent(
      String stringValue, int intValue, NessieFileContentType nessieFileContentType) {
    this.stringValue = stringValue;
    this.intValue = intValue;
    this.nessieFileContentType = nessieFileContentType;
  }

  public static IcebergManifestContent fromStringValue(String content) {
    switch (content) {
      case "data":
        return DATA;
      case "deletes":
        return DELETES;
      default:
        throw new IllegalStateException("Illegal value for 'content', must be 'data' or 'deletes'");
    }
  }

  public String stringValue() {
    return stringValue;
  }

  public int intValue() {
    return intValue;
  }

  public NessieFileContentType nessieFileContentType() {
    return nessieFileContentType;
  }

  public static IcebergManifestContent fromNessieFileContentType(
      NessieFileContentType nessieFileContentType) {
    switch (nessieFileContentType) {
      case ICEBERG_DATA_FILE:
        return DATA;
      case ICEBERG_DELETE_FILE:
        return DELETES;
      default:
        throw new IllegalArgumentException(
            "Unknown file content type for Iceberg: " + nessieFileContentType);
    }
  }
}
