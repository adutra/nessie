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
  id("nessie-conventions-quarkus")
  id("nessie-jacoco")
}

extra["maven.name"] = "Nessie - Catalog - Server Tests"

dependencies {
  implementation(project(":nessie-client"))
  implementation(project(":nessie-model"))
  implementation(project(":nessie-catalog-format-iceberg"))
  implementation(project(":nessie-catalog-format-iceberg-fixturegen"))

  implementation(libs.avro)

  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation(libs.vertx.core)

  implementation(platform(libs.junit.bom))
  implementation(libs.bundles.junit.testing)

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  compileOnly(libs.microprofile.openapi)
}
