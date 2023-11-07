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
  id("nessie-conventions-server")
  id("nessie-jacoco")
  alias(libs.plugins.jmh)
}

extra["maven.name"] = "Nessie - Catalog - Storage Persist"

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-catalog-schema-model"))
  implementation(project(":nessie-catalog-storage-backend"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(path = ":nessie-protobuf-relocated", configuration = "shadow"))

  compileOnly(project(":nessie-immutables"))

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-smile")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-guava")

  // javax/jakarta
  compileOnly(libs.jakarta.ws.rs.api)
  compileOnly(libs.javax.ws.rs)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.javax.enterprise.cdi.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.javax.validation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(libs.errorprone.annotations)
  compileOnly(libs.microprofile.openapi)

  jmhImplementation(project(":nessie-catalog-format-iceberg-fixturegen"))
  jmhImplementation(project(":nessie-catalog-format-iceberg"))
  jmhImplementation(platform(libs.jackson.bom))
  jmhImplementation("com.fasterxml.jackson.dataformat:jackson-dataformat-ion")
  jmhImplementation("com.fasterxml.jackson.dataformat:jackson-dataformat-cbor")
  jmhImplementation(libs.zstd.jni)
  jmhImplementation(libs.snappy.java)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)
}

jmh { jmhVersion.set(libs.versions.jmh.get()) }
