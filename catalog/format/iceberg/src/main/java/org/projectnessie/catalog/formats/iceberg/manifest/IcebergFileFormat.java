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

import org.projectnessie.catalog.model.manifest.NessieDataFileFormat;

public enum IcebergFileFormat {
  ORC("orc", true, NessieDataFileFormat.ORC),
  PARQUET("parquet", true, NessieDataFileFormat.PARQUET),
  AVRO("avro", true, NessieDataFileFormat.AVRO),
  METADATA("metadata.json", false, NessieDataFileFormat.METADATA);

  private final String fileExtension;
  private final boolean splittable;
  private final NessieDataFileFormat nessieDataFileFormat;

  IcebergFileFormat(
      String fileExtension, boolean splittable, NessieDataFileFormat nessieDataFileFormat) {
    this.fileExtension = "." + fileExtension;
    this.splittable = splittable;
    this.nessieDataFileFormat = nessieDataFileFormat;
  }

  public boolean splittable() {
    return splittable;
  }

  public String fileExtension() {
    return fileExtension;
  }

  public NessieDataFileFormat nessieDataFileFormat() {
    return nessieDataFileFormat;
  }

  public static IcebergFileFormat fromNessieDataFileFormat(
      NessieDataFileFormat nessieDataFileFormat) {
    switch (nessieDataFileFormat) {
      case AVRO:
        return AVRO;
      case ORC:
        return ORC;
      case METADATA:
        return METADATA;
      case PARQUET:
        return PARQUET;
      default:
        throw new IllegalArgumentException(
            "Unknown data file format for Iceberg: " + nessieDataFileFormat);
    }
  }
}
