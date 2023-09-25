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
package org.projectnessie.catalog.formats.iceberg.meta;

public final class IcebergTableProperties {
  private IcebergTableProperties() {}

  public static final String AVRO_COMPRESSION = "write.avro.compression-codec";
  public static final String DELETE_AVRO_COMPRESSION = "write.delete.avro.compression-codec";
  public static final String AVRO_COMPRESSION_DEFAULT = "gzip";

  public static final String AVRO_COMPRESSION_LEVEL = "write.avro.compression-level";
  public static final String DELETE_AVRO_COMPRESSION_LEVEL = "write.delete.avro.compression-level";
  public static final String AVRO_COMPRESSION_LEVEL_DEFAULT = null;
}
