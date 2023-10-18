/*
 * Copyright (C) 2022 Dremio
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

plugins {
  id("nessie-conventions-client")
  id("nessie-jacoco")
}

description = "Nessie - Catalog - Iceberg HTTP FileIO"

val versionIceberg = libs.versions.iceberg.get()

dependencies {
  compileOnly(libs.slf4j.api)
  compileOnly(libs.hadoop.common)
  compileOnly("org.apache.iceberg:iceberg-core:$versionIceberg")
  compileOnly("org.apache.iceberg:iceberg-bundled-guava:$versionIceberg")
}
