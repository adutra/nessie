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
package org.projectnessie.catalog.model.manifest;

public enum NessieDataFileFormat {
  ORC("orc", true, "application/orc"),
  PARQUET("parquet", true, "application/parquet"),
  AVRO("avro", true, "avro/binary"),
  METADATA("metadata.json", false, "application/json"),
  ;

  private final String fileExtension;
  private final boolean splittable;
  private final String contentType;

  NessieDataFileFormat(String fileExtension, boolean splittable, String contentType) {
    this.fileExtension = "." + fileExtension;
    this.splittable = splittable;
    this.contentType = contentType;
  }

  public boolean splittable() {
    return splittable;
  }

  public String fileExtension() {
    return fileExtension;
  }

  public String contentType() {
    return contentType;
  }

  public static NessieDataFileFormat forFileName(String fileName) {
    for (NessieDataFileFormat value : values()) {
      if (fileName.endsWith(value.fileExtension)) {
        return value;
      }
    }
    return null;
  }
}
